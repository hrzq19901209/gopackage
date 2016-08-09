package main

import (
	"archive/zip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	notaryclient "github.com/docker/notary/client"
	"github.com/docker/notary/cryptoservice"
	"github.com/docker/notary/passphrase"
	"github.com/docker/notary/trustmanager"

	"github.com/docker/notary"
	"github.com/docker/notary/tuf/data"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cmdKeyTemplate = usageTemplate{
	Use:   "key",
	Short: "Operates on keys.",
	Long:  `Operations on private keys.`,
}

var cmdKeyListTemplate = usageTemplate{
	Use:   "list",
	Short: "Lists keys.",
	Long:  "Lists all keys known to notary.",
}

var cmdRotateKeyTemplate = usageTemplate{
	Use:   "rotate [ GUN ] [ key role ]",
	Short: "Rotate a signing (non-root) key of the given type for the given Globally Unique Name and role.",
	Long:  `Generates a new key for the given Globally Unique Name and role (one of "snapshot", "targets", "root", or "timestamp").  If rotating to a server-managed key, a new key is requested from the server rather than generated.  If the generation or key request is successful, the key rotation is immediately published.  No other changes, even if they are staged, will be published.`,
}

var cmdKeyGenerateRootKeyTemplate = usageTemplate{
	Use:   "generate [ algorithm ]",
	Short: "Generates a new root key with a given algorithm.",
	Long:  "Generates a new root key with a given algorithm. If hardware key storage (e.g. a Yubikey) is available, the key will be stored both on hardware and on disk (so that it can be backed up).  Please make sure to back up and then remove this on-key disk immediately afterwards.",
}

var cmdKeysBackupTemplate = usageTemplate{
	Use:   "backup [ zipfilename ]",
	Short: "Backs up all your on-disk keys to a ZIP file.",
	Long:  "Backs up all of your accessible of keys. The keys are reencrypted with a new passphrase. The output is a ZIP file.  If the --gun option is passed, only signing keys and no root keys will be backed up.  Does not work on keys that are only in hardware (e.g. Yubikeys).",
}

var cmdKeyExportTemplate = usageTemplate{
	Use:   "export [ keyID ] [ pemfilename ]",
	Short: "Export a private key on disk to a PEM file.",
	Long:  "Exports a single private key on disk, without reencrypting. The output is a PEM file. Does not work on keys that are only in hardware (e.g. Yubikeys).",
}

var cmdKeysRestoreTemplate = usageTemplate{
	Use:   "restore [ zipfilename ]",
	Short: "Restore multiple keys from a ZIP file.",
	Long:  "Restores one or more keys from a ZIP file. If hardware key storage (e.g. a Yubikey) is available, root keys will be imported into the hardware, but not backed up to disk in the same location as the other, non-root keys.",
}

var cmdKeyImportTemplate = usageTemplate{
	Use:   "import [ pemfilename ]",
	Short: "Imports a key from a PEM file.",
	Long:  "Imports a single key from a PEM file. If a hardware key storage (e.g. Yubikey) is available, the root key will be imported into the hardware but not backed up on disk again.",
}

var cmdKeyRemoveTemplate = usageTemplate{
	Use:   "remove [ keyID ]",
	Short: "Removes the key with the given keyID.",
	Long:  "Removes the key with the given keyID.  If the key is stored in more than one location, you will be asked which one to remove.",
}

var cmdKeyPasswdTemplate = usageTemplate{
	Use:   "passwd [ keyID ]",
	Short: "Changes the passphrase for the key with the given keyID.",
	Long:  "Changes the passphrase for the key with the given keyID.  Will require validation of the old passphrase.",
}

type keyCommander struct {
	// these need to be set
	configGetter func() (*viper.Viper, error)
	getRetriever func() passphrase.Retriever

	// these are for command line parsing - no need to set
	keysExportChangePassphrase bool
	keysExportGUN              string
	keysImportGUN              string
	keysImportRole             string
	rotateKeyRole              string
	rotateKeyServerManaged     bool

	input io.Reader
}

func (k *keyCommander) GetCommand() *cobra.Command {
	cmd := cmdKeyTemplate.ToCommand(nil)
	cmd.AddCommand(cmdKeyListTemplate.ToCommand(k.keysList))
	cmd.AddCommand(cmdKeyGenerateRootKeyTemplate.ToCommand(k.keysGenerateRootKey))
	cmd.AddCommand(cmdKeysRestoreTemplate.ToCommand(k.keysRestore))
	cmdKeysImport := cmdKeyImportTemplate.ToCommand(k.keysImport)
	cmdKeysImport.Flags().StringVarP(
		&k.keysImportGUN, "gun", "g", "", "Globally Unique Name to import key to")
	cmdKeysImport.Flags().StringVarP(
		&k.keysImportRole, "role", "r", "", "Role to import key to (if not in PEM headers)")
	cmd.AddCommand(cmdKeysImport)

	cmd.AddCommand(cmdKeyRemoveTemplate.ToCommand(k.keyRemove))
	cmd.AddCommand(cmdKeyPasswdTemplate.ToCommand(k.keyPassphraseChange))

	cmdKeysBackup := cmdKeysBackupTemplate.ToCommand(k.keysBackup)
	cmdKeysBackup.Flags().StringVarP(
		&k.keysExportGUN, "gun", "g", "", "Globally Unique Name to export keys for")
	cmd.AddCommand(cmdKeysBackup)

	cmdKeyExport := cmdKeyExportTemplate.ToCommand(k.keysExport)
	cmdKeyExport.Flags().BoolVarP(
		&k.keysExportChangePassphrase, "change-passphrase", "p", false,
		"Set a new passphrase for the key being exported")
	cmd.AddCommand(cmdKeyExport)

	cmdRotateKey := cmdRotateKeyTemplate.ToCommand(k.keysRotate)
	cmdRotateKey.Flags().BoolVarP(&k.rotateKeyServerManaged, "server-managed", "r",
		false, "Signing and key management will be handled by the remote server "+
			"(no key will be generated or stored locally). "+
			"Required for timestamp role, optional for snapshot role")
	cmd.AddCommand(cmdRotateKey)

	return cmd
}

func (k *keyCommander) keysList(cmd *cobra.Command, args []string) error {
	if len(args) > 0 {
		cmd.Usage()
		return fmt.Errorf("")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, false)
	if err != nil {
		return err
	}

	cmd.Println("")
	prettyPrintKeys(ks, cmd.Out())
	cmd.Println("")
	return nil
}

func (k *keyCommander) keysGenerateRootKey(cmd *cobra.Command, args []string) error {
	// We require one or no arguments (since we have a default value), but if the
	// user passes in more than one argument, we error out.
	if len(args) > 1 {
		cmd.Usage()
		return fmt.Errorf(
			"Please provide only one Algorithm as an argument to generate (rsa, ecdsa)")
	}

	// If no param is given to generate, generates an ecdsa key by default
	algorithm := data.ECDSAKey

	// If we were provided an argument lets attempt to use it as an algorithm
	if len(args) > 0 {
		algorithm = args[0]
	}

	allowedCiphers := map[string]bool{
		data.ECDSAKey: true,
		data.RSAKey:   true,
	}

	if !allowedCiphers[strings.ToLower(algorithm)] {
		return fmt.Errorf("Algorithm not allowed, possible values are: RSA, ECDSA")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, true)
	if err != nil {
		return err
	}
	cs := cryptoservice.NewCryptoService(ks...)

	pubKey, err := cs.Create(data.CanonicalRootRole, "", algorithm)
	if err != nil {
		return fmt.Errorf("Failed to create a new root key: %v", err)
	}

	cmd.Printf("Generated new %s root key with keyID: %s\n", algorithm, pubKey.ID())
	return nil
}

// keysBackup exports a collection of keys to a ZIP file
func (k *keyCommander) keysBackup(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		cmd.Usage()
		return fmt.Errorf("Must specify output filename for export")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, false, false)
	if err != nil {
		return err
	}
	exportFilename := args[0]

	cs := cryptoservice.NewCryptoService(ks...)

	exportFile, err := os.Create(exportFilename)
	if err != nil {
		return fmt.Errorf("Error creating output file: %v", err)
	}

	// Must use a different passphrase retriever to avoid caching the
	// unlocking passphrase and reusing that.
	exportRetriever := k.getRetriever()
	if k.keysExportGUN != "" {
		err = cs.ExportKeysByGUN(exportFile, k.keysExportGUN, exportRetriever)
	} else {
		err = cs.ExportAllKeys(exportFile, exportRetriever)
	}

	exportFile.Close()

	if err != nil {
		os.Remove(exportFilename)
		return fmt.Errorf("Error exporting keys: %v", err)
	}
	return nil
}

// keysExport exports a key by ID to a PEM file
func (k *keyCommander) keysExport(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		cmd.Usage()
		return fmt.Errorf("Must specify key ID and output filename for export")
	}

	keyID := args[0]
	exportFilename := args[1]

	if len(keyID) != notary.Sha256HexSize {
		return fmt.Errorf("Please specify a valid key ID")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, false)
	if err != nil {
		return err
	}

	cs := cryptoservice.NewCryptoService(ks...)
	keyInfo, err := cs.GetKeyInfo(keyID)
	if err != nil {
		return fmt.Errorf("Could not retrieve info for key %s", keyID)
	}

	exportFile, err := os.Create(exportFilename)
	if err != nil {
		return fmt.Errorf("Error creating output file: %v", err)
	}
	if k.keysExportChangePassphrase {
		// Must use a different passphrase retriever to avoid caching the
		// unlocking passphrase and reusing that.
		exportRetriever := k.getRetriever()
		err = cs.ExportKeyReencrypt(exportFile, keyID, exportRetriever)
	} else {
		err = cs.ExportKey(exportFile, keyID, keyInfo.Role)
	}
	exportFile.Close()
	if err != nil {
		os.Remove(exportFilename)
		return fmt.Errorf("Error exporting %s key: %v", keyInfo.Role, err)
	}
	return nil
}

// keysRestore imports keys from a ZIP file
func (k *keyCommander) keysRestore(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		cmd.Usage()
		return fmt.Errorf("Must specify input filename for import")
	}

	importFilename := args[0]

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, false)
	if err != nil {
		return err
	}
	cs := cryptoservice.NewCryptoService(ks...)

	zipReader, err := zip.OpenReader(importFilename)
	if err != nil {
		return fmt.Errorf("Opening file for import: %v", err)
	}
	defer zipReader.Close()

	err = cs.ImportKeysZip(zipReader.Reader, k.getRetriever())

	if err != nil {
		return fmt.Errorf("Error importing keys: %v", err)
	}
	return nil
}

// keysImport imports a private key from a PEM file for a role
func (k *keyCommander) keysImport(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		cmd.Usage()
		return fmt.Errorf("Must specify input filename for import")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, false)
	if err != nil {
		return err
	}

	importFilename := args[0]

	importFile, err := os.Open(importFilename)
	if err != nil {
		return fmt.Errorf("Opening file for import: %v", err)
	}
	defer importFile.Close()

	pemBytes, err := ioutil.ReadAll(importFile)
	if err != nil {
		return fmt.Errorf("Error reading input file: %v", err)
	}

	pemRole := trustmanager.ReadRoleFromPEM(pemBytes)

	// If the PEM key doesn't have a role in it, we must have --role set
	if pemRole == "" && k.keysImportRole == "" {
		return fmt.Errorf("Could not infer role, and no role was specified for key")
	}

	// If both  PEM role and a --role are provided and they don't match, error
	if pemRole != "" && k.keysImportRole != "" && pemRole != k.keysImportRole {
		return fmt.Errorf("Specified role %s does not match role %s in PEM headers", k.keysImportRole, pemRole)
	}

	// Determine which role to add to between PEM headers and --role flag:
	var importRole string
	if k.keysImportRole != "" {
		importRole = k.keysImportRole
	} else {
		importRole = pemRole
	}

	// If we're importing to targets or snapshot, we need a GUN
	if (importRole == data.CanonicalTargetsRole || importRole == data.CanonicalSnapshotRole) && k.keysImportGUN == "" {
		return fmt.Errorf("Must specify GUN for %s key", importRole)
	}

	// Root keys must be encrypted
	if importRole == data.CanonicalRootRole {
		if err = cryptoservice.CheckRootKeyIsEncrypted(pemBytes); err != nil {
			return err
		}
	}

	cs := cryptoservice.NewCryptoService(ks...)
	// Convert to a data.PrivateKey, potentially decrypting the key
	privKey, err := trustmanager.ParsePEMPrivateKey(pemBytes, "")
	if err != nil {
		privKey, _, err = trustmanager.GetPasswdDecryptBytes(k.getRetriever(), pemBytes, "", "imported "+importRole)
		if err != nil {
			return err
		}
	}
	err = cs.AddKey(importRole, k.keysImportGUN, privKey)
	if err != nil {
		return fmt.Errorf("Error importing key: %v", err)
	}
	return nil
}

func (k *keyCommander) keysRotate(cmd *cobra.Command, args []string) error {
	if len(args) < 2 {
		cmd.Usage()
		return fmt.Errorf("Must specify a GUN and a key role to rotate")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}

	gun := args[0]
	rotateKeyRole := args[1]

	rt, err := getTransport(config, gun, false)
	if err != nil {
		return err
	}

	trustPin, err := getTrustPinning(config)
	if err != nil {
		return err
	}

	nRepo, err := notaryclient.NewNotaryRepository(
		config.GetString("trust_dir"), gun, getRemoteTrustServer(config),
		rt, k.getRetriever(), trustPin)
	if err != nil {
		return err
	}

	if rotateKeyRole == data.CanonicalRootRole {
		cmd.Print("Warning: you are about to rotate your root key.\n\n" +
			"You must use your old key to sign this root rotation. We recommend that\n" +
			"you sign all your future root changes with this key as well, so that\n" +
			"clients can have a smoother update process. Please do not delete\n" +
			"this key after rotating.\n\n" +
			"Are you sure you want to proceed?  (yes/no)  ")

		if !askConfirm(k.input) {
			fmt.Fprintln(cmd.Out(), "\nAborting action.")
			return nil
		}
	}

	return nRepo.RotateKey(rotateKeyRole, k.rotateKeyServerManaged)
}

func removeKeyInteractively(keyStores []trustmanager.KeyStore, keyID string,
	in io.Reader, out io.Writer) error {

	var foundKeys [][]string
	var storesByIndex []trustmanager.KeyStore

	for _, store := range keyStores {
		for keypath, keyInfo := range store.ListKeys() {
			if filepath.Base(keypath) == keyID {
				foundKeys = append(foundKeys,
					[]string{keypath, keyInfo.Role, store.Name()})
				storesByIndex = append(storesByIndex, store)
			}
		}
	}

	if len(foundKeys) == 0 {
		return fmt.Errorf("No key with ID %s found.", keyID)
	}

	if len(foundKeys) > 1 {
		for {
			// ask the user for which key to delete
			fmt.Fprintf(out, "Found the following matching keys:\n")
			for i, info := range foundKeys {
				fmt.Fprintf(out, "\t%d. %s: %s (%s)\n", i+1, info[0], info[1], info[2])
			}
			fmt.Fprint(out, "Which would you like to delete?  Please enter a number:  ")
			var result string
			if _, err := fmt.Fscanln(in, &result); err != nil {
				return err
			}
			index, err := strconv.Atoi(strings.TrimSpace(result))

			if err != nil || index > len(foundKeys) || index < 1 {
				fmt.Fprintf(out, "\nInvalid choice: %s\n", string(result))
				continue
			}
			foundKeys = [][]string{foundKeys[index-1]}
			storesByIndex = []trustmanager.KeyStore{storesByIndex[index-1]}
			fmt.Fprintln(out, "")
			break
		}
	}
	// Now the length must be 1 - ask for confirmation.
	keyDescription := fmt.Sprintf("%s (role %s) from %s", foundKeys[0][0],
		foundKeys[0][1], foundKeys[0][2])

	fmt.Fprintf(out, "Are you sure you want to remove %s?  (yes/no)  ",
		keyDescription)
	if !askConfirm(in) {
		fmt.Fprintln(out, "\nAborting action.")
		return nil
	}

	if err := storesByIndex[0].RemoveKey(foundKeys[0][0]); err != nil {
		return err
	}

	fmt.Fprintf(out, "\nDeleted %s.\n", keyDescription)
	return nil
}

// keyRemove deletes a private key based on ID
func (k *keyCommander) keyRemove(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		cmd.Usage()
		return fmt.Errorf("must specify the key ID of the key to remove")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, false)
	if err != nil {
		return err
	}
	keyID := args[0]

	// This is an invalid ID
	if len(keyID) != notary.Sha256HexSize {
		return fmt.Errorf("invalid key ID provided: %s", keyID)
	}
	cmd.Println("")
	err = removeKeyInteractively(ks, keyID, k.input, cmd.Out())
	cmd.Println("")
	return err
}

// keyPassphraseChange changes the passphrase for a private key based on ID
func (k *keyCommander) keyPassphraseChange(cmd *cobra.Command, args []string) error {
	if len(args) < 1 {
		cmd.Usage()
		return fmt.Errorf("must specify the key ID of the key to change the passphrase of")
	}

	config, err := k.configGetter()
	if err != nil {
		return err
	}
	ks, err := k.getKeyStores(config, true, false)
	if err != nil {
		return err
	}

	keyID := args[0]

	// This is an invalid ID
	if len(keyID) != notary.Sha256HexSize {
		return fmt.Errorf("invalid key ID provided: %s", keyID)
	}

	// Find which keyStore we should replace the key password in, and replace if we find it
	var foundKeyStore trustmanager.KeyStore
	var privKey data.PrivateKey
	var keyInfo trustmanager.KeyInfo
	var cs *cryptoservice.CryptoService
	for _, keyStore := range ks {
		cs = cryptoservice.NewCryptoService(keyStore)
		if privKey, _, err = cs.GetPrivateKey(keyID); err == nil {
			foundKeyStore = keyStore
			break
		}
	}
	if foundKeyStore == nil {
		return fmt.Errorf("could not retrieve local key for key ID provided: %s", keyID)
	}
	// Must use a different passphrase retriever to avoid caching the
	// unlocking passphrase and reusing that.
	passChangeRetriever := k.getRetriever()
	var addingKeyStore trustmanager.KeyStore
	switch foundKeyStore.Name() {
	case "yubikey":
		addingKeyStore, err = getYubiStore(nil, passChangeRetriever)
		keyInfo = trustmanager.KeyInfo{Role: data.CanonicalRootRole}
	default:
		addingKeyStore, err = trustmanager.NewKeyFileStore(config.GetString("trust_dir"), passChangeRetriever)
		if err != nil {
			return err
		}
		keyInfo, err = foundKeyStore.GetKeyInfo(keyID)
	}
	if err != nil {
		return err
	}
	err = addingKeyStore.AddKey(keyInfo, privKey)
	if err != nil {
		return err
	}
	cmd.Println("")
	cmd.Printf("Successfully updated passphrase for key ID: %s", keyID)
	cmd.Println("")
	return nil
}

func (k *keyCommander) getKeyStores(
	config *viper.Viper, withHardware, hardwareBackup bool) ([]trustmanager.KeyStore, error) {
	retriever := k.getRetriever()

	directory := config.GetString("trust_dir")
	fileKeyStore, err := trustmanager.NewKeyFileStore(directory, retriever)
	if err != nil {
		return nil, fmt.Errorf(
			"Failed to create private key store in directory: %s", directory)
	}

	ks := []trustmanager.KeyStore{fileKeyStore}

	if withHardware {
		var yubiStore trustmanager.KeyStore
		if hardwareBackup {
			yubiStore, err = getYubiStore(fileKeyStore, retriever)
		} else {
			yubiStore, err = getYubiStore(nil, retriever)
		}
		if err == nil && yubiStore != nil {
			// Note that the order is important, since we want to prioritize
			// the yubikey store
			ks = []trustmanager.KeyStore{yubiStore, fileKeyStore}
		}
	}

	return ks, nil
}
