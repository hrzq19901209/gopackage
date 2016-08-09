package protocol

import (
	"container/heap"
	"testing"
)

func TestQueueWithParent(t *testing.T) {
	var q JobQueue
	ids := []string{
		"2016/01/28 20:48:14 10",
		"2016/01/28 20:47:32 1",
		"2016/01/28 20:48:09 9",
		"2016/01/28 20:47:32 2",
	}
	for _, id := range ids {
		heap.Push(&q, &Master{Job: &BasicJob{Parent: id}})
	}

	target := []int{1, 3, 2, 0}
	for i := 0; i < 4; i++ {
		r := heap.Pop(&q).(*Master).GetParent()
		if r != ids[target[i]] {
			t.Errorf("expect %s, got %s", ids[target[i]], r)
		}
	}
}

func TestQueueWithoutParent(t *testing.T) {
	var q JobQueue
	ids := []string{
		"2016/01/28 20:48:14 10",
		"2016/01/28 20:47:32 1",
		"2016/01/28 20:48:09 9",
		"2016/01/28 20:47:32 2",
	}
	for _, id := range ids {
		heap.Push(&q, &Master{Job: &BasicJob{ID: id}})
	}

	target := []int{1, 3, 2, 0}
	for i := 0; i < 4; i++ {
		r := heap.Pop(&q).(*Master).GetID()
		if r != ids[target[i]] {
			t.Errorf("expect %s, got %s", ids[target[i]], r)
		}
	}
}

func TestQueueMixed(t *testing.T) {
	var q JobQueue
	ids := []string{ "0", "1", "2", "3"};
	parents := []string {
		"2016/01/28 20:48:14 10",
		"2016/01/28 20:47:32 1",
		"",
		"",
	}
	for i := 0; i < 4; i++ {
		heap.Push(&q, &Master{Job: &BasicJob{ID: ids[i], Parent: parents[i]}})
	}

	target := []int{1, 0, 2, 3}
	for i := 0; i < 4; i++ {
		r := heap.Pop(&q).(*Master).GetID()
		if r != ids[target[i]] {
			t.Errorf("expect %s, got %s", ids[target[i]], r)
		}
	}
}
