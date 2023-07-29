package list

import "container/list"

// pageSize must be even
const pageSize = 1024

// QuickList is a linked list of page (which type is []interface{})
// QuickList has better performance than LinkedList of Add, Range and memory usage
type QuickList struct {
	data *list.List // list of []interface{}
	size int
}

// iterator of QuickList, move between [-1, ql.Len()]
type iterator struct {
	node   *list.Element
	offset int
	ql     *QuickList
}

func NewQuickList() *QuickList {
	l := &QuickList{
		data: list.New(),
	}
	return l
}

// Add adds value to the tail
func (ql *QuickList) Add(val interface{}) {
	ql.size++
	if ql.data.Len() == 0 { // empty list
		page := make([]interface{}, 0, pageSize) // 双向链表的每一个节点就是一个 固定大小的切片
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	// assert list.data.Back() != nil
	backNode := ql.data.Back()
	backPage := backNode.Value.([]interface{}) // 最后一个节点的value
	if len(backPage) == cap(backPage) {        // full page, create new page
		page := make([]interface{}, 0, pageSize)
		page = append(page, val)
		ql.data.PushBack(page)
		return
	}
	// append into page
	backPage = append(backPage, val)
	backNode.Value = backPage
}

// 为什么quickList的find要更快。
// find returns page and in-page-offset of given index
func (ql *QuickList) find(index int) *iterator {
	if ql == nil {
		panic("list is nil")
	}
	if index < 0 || index >= ql.size {
		panic("index out of bound")
	}
	var n *list.Element
	var page []interface{}
	var pageBeg int
	if index < ql.size/2 {
		// search from front
		n = ql.data.Front()
		pageBeg = 0
		for {
			// assert: n != nil
			page = n.Value.([]interface{})
			if pageBeg+len(page) > index { // 在不在这个节点里面
				break
			}
			pageBeg += len(page)
			n = n.Next()
		}
	} else {
		// search from back
		n = ql.data.Back()
		pageBeg = ql.size
		for {
			page = n.Value.([]interface{})
			pageBeg -= len(page)
			if pageBeg <= index {
				break
			}
			n = n.Prev()
		}
	}
	pageOffset := index - pageBeg
	return &iterator{
		node:   n,
		offset: pageOffset,
		ql:     ql,
	}
}

func (iter *iterator) get() interface{} {
	return iter.page()[iter.offset]
}

func (iter *iterator) page() []interface{} {
	return iter.node.Value.([]interface{})
}

// next returns whether iter is in bound
func (iter *iterator) next() bool {
	page := iter.page()
	if iter.offset < len(page)-1 {
		iter.offset++
		return true
	}
	// move to next page
	if iter.node == iter.ql.data.Back() {
		// already at last node
		iter.offset = len(page)
		return false
	}
	iter.offset = 0
	iter.node = iter.node.Next()
	return true
}

// prev returns whether iter is in bound
func (iter *iterator) prev() bool {
	if iter.offset > 0 {
		iter.offset--
		return true
	}
	// move to prev page
	if iter.node == iter.ql.data.Front() {
		// already at first page
		iter.offset = -1
		return false
	}
	iter.node = iter.node.Prev()
	prevPage := iter.node.Value.([]interface{})
	iter.offset = len(prevPage) - 1
	return true
}

func (iter *iterator) atEnd() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Back() {
		return false
	}
	page := iter.page()
	return iter.offset == len(page)
}

func (iter *iterator) atBegin() bool {
	if iter.ql.data.Len() == 0 {
		return true
	}
	if iter.node != iter.ql.data.Front() {
		return false
	}
	return iter.offset == -1
}

// Get returns value at the given index
func (ql *QuickList) Get(index int) (val interface{}) {
	iter := ql.find(index)
	return iter.get()
}

func (iter *iterator) set(val interface{}) {
	page := iter.page()
	page[iter.offset] = val
}

// Set updates value at the given index, the index should between [0, list.size]
func (ql *QuickList) Set(index int, val interface{}) {
	iter := ql.find(index)
	iter.set(val)
}

func (ql *QuickList) Insert(index int, val interface{}) {
	if index == ql.size { // 插入位置等于长度，也就是插在尾部
		ql.Add(val)
		return
	}
	iter := ql.find(index)                  // quickList 的find更快
	page := iter.node.Value.([]interface{}) // 把接口切片取出来
	if len(page) < pageSize {
		// insert into not full page
		page = append(page[:iter.offset+1], page[iter.offset:]...)
		page[iter.offset] = val
		iter.node.Value = page
		ql.size++
		return
	}
	// insert into a full page may cause memory copy, so we split a full page into two half pages
	// 可以只复制一半元素就可以，减少复制开销，同时留出空间，避免频繁的进行内存复制，后续插入的时候不需要复制，但是缺点就是浪费了一部分内存空间。空间换时间。
	var nextPage []interface{}
	nextPage = append(nextPage, page[pageSize/2:]...) // pageSize must be even  后半段进行了复制
	page = page[:pageSize/2]                          // 前半段没有复制
	if iter.offset < len(page) {                      // 如果小于一半
		page = append(page[:iter.offset+1], page[iter.offset:]...) // 插入到前半段
		page[iter.offset] = val
	} else {
		i := iter.offset - pageSize/2
		nextPage = append(nextPage[:i+1], nextPage[i:]...) // 插入到后半段
		nextPage[i] = val
	}
	// store current page and next page
	iter.node.Value = page                   // 前半段
	ql.data.InsertAfter(nextPage, iter.node) // 把后半段这个节点插入到双向链表中
	ql.size++
}

func (iter *iterator) remove() interface{} {
	page := iter.page()
	val := page[iter.offset]
	page = append(page[:iter.offset], page[iter.offset+1:]...)
	if len(page) > 0 {
		// page is not empty, update iter.offset only
		iter.node.Value = page
		if iter.offset == len(page) {
			// removed page[-1], node should move to next page
			if iter.node != iter.ql.data.Back() {
				iter.node = iter.node.Next()
				iter.offset = 0
			}
			// else: assert iter.atEnd() == true
		}
	} else {
		// page is empty, update iter.node and iter.offset
		if iter.node == iter.ql.data.Back() {
			// removed last element, ql is empty now
			iter.ql.data.Remove(iter.node)
			iter.node = nil
			iter.offset = 0
		} else {
			nextNode := iter.node.Next()
			iter.ql.data.Remove(iter.node)
			iter.node = nextNode
			iter.offset = 0
		}
	}
	iter.ql.size--
	return val
}

// Remove removes value at the given index
func (ql *QuickList) Remove(index int) interface{} {
	iter := ql.find(index)
	return iter.remove()
}

// Len returns the number of elements in list
func (ql *QuickList) Len() int {
	return ql.size
}

// RemoveLast removes the last element and returns its value
func (ql *QuickList) RemoveLast() interface{} {
	if ql.Len() == 0 {
		return nil
	}
	ql.size--
	lastNode := ql.data.Back()
	lastPage := lastNode.Value.([]interface{})
	if len(lastPage) == 1 {
		ql.data.Remove(lastNode)
		return lastPage[0]
	}
	val := lastPage[len(lastPage)-1]
	lastPage = lastPage[:len(lastPage)-1]
	lastNode.Value = lastPage
	return val
}

// RemoveAllByVal removes all elements with the given val
func (ql *QuickList) RemoveAllByVal(expected Expected) int {
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
		} else {
			iter.next()
		}
	}
	return removed
}

// RemoveByVal removes at most `count` values of the specified value in this list
// scan from left to right
func (ql *QuickList) RemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(0)
	removed := 0
	for !iter.atEnd() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		} else {
			iter.next()
		}
	}
	return removed
}

func (ql *QuickList) ReverseRemoveByVal(expected Expected, count int) int {
	if ql.size == 0 {
		return 0
	}
	iter := ql.find(ql.size - 1)
	removed := 0
	for !iter.atBegin() {
		if expected(iter.get()) {
			iter.remove()
			removed++
			if removed == count {
				break
			}
		}
		iter.prev()
	}
	return removed
}

// ForEach visits each element in the list
// if the consumer returns false, the loop will be break
func (ql *QuickList) ForEach(consumer Consumer) {
	if ql == nil {
		panic("list is nil")
	}
	if ql.Len() == 0 {
		return
	}
	iter := ql.find(0)
	i := 0
	for {
		goNext := consumer(i, iter.get())
		if !goNext {
			break
		}
		i++
		if !iter.next() {
			break
		}
	}
}

func (ql *QuickList) Contains(expected Expected) bool {
	contains := false
	ql.ForEach(func(i int, actual interface{}) bool {
		if expected(actual) {
			contains = true
			return false
		}
		return true
	})
	return contains
}

// Range returns elements which index within [start, stop)
func (ql *QuickList) Range(start int, stop int) []interface{} {
	if start < 0 || start >= ql.Len() {
		panic("`start` out of range")
	}
	if stop < start || stop > ql.Len() {
		panic("`stop` out of range")
	}
	sliceSize := stop - start
	slice := make([]interface{}, 0, sliceSize)
	iter := ql.find(start)
	i := 0
	for i < sliceSize {
		slice = append(slice, iter.get())
		iter.next()
		i++
	}
	return slice
}
