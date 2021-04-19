import heapq
def queue_remove_entry_for(self, heap, sender):
    heap = self.procqu
    reqi = next(filter(lambda i: heap[i][1]==sender, range(0, len(heap))), -1)
    if reqi == -1:
        raise Exception("Request not in list")
    else:
        heap[reqi] = self.procqu[-1]
        heap.pop()
        heapq.heapify(self.procqu)