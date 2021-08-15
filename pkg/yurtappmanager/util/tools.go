/*
Copyright 2020 The OpenYurt Authors.
Copyright 2019 The Kruise Authors.
Copyright 2016 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package util

import (
	"sync"

	"k8s.io/utils/integer"
)

// SlowStartBatch tries to call the provided function a total of 'count' times,
// starting slow to check for errors, then speeding up if calls succeed.
//
// It groups the calls into batches, starting with a group of initialBatchSize.
// Within each batch, it may call the function multiple times concurrently with its index.
//
// If a whole batch succeeds, the next batch may get exponentially larger.
// If there are any failures in a batch, all remaining batches are skipped
// after waiting for the current batch to complete.
//
// It returns the number of successful calls to the function.
// SlowStartBatch尝试调用所提供的函数的总数为“count”次，开始缓慢地检查错误，然后在调用成功时加速。
// 它将调用分组为批，从一组initialBatchSize开始。在每个批处理中，它可以使用其索引并发地多次调用函数。
// 如果整批成功，下一批可能会成倍地变大。如果某个批处理中存在失败，则在等待当前批处理完成后跳过所有剩余的批处理。
// 它返回成功调用函数的次数。
func SlowStartBatch(count int, initialBatchSize int, fn func(index int) error) (int, error) {
	remaining := count
	successes := 0
	index := 0
	for batchSize := integer.IntMin(remaining, initialBatchSize); batchSize > 0; batchSize = integer.IntMin(2*batchSize, remaining) {
		errCh := make(chan error, batchSize)
		var wg sync.WaitGroup
		wg.Add(batchSize)
		for i := 0; i < batchSize; i++ {
			go func(idx int) {
				defer wg.Done()
				if err := fn(idx); err != nil {
					errCh <- err
				}
			}(index)
			index++
		}
		wg.Wait()
		curSuccesses := batchSize - len(errCh)
		successes += curSuccesses
		if len(errCh) > 0 {
			return successes, <-errCh
		}
		remaining -= batchSize
	}
	return successes, nil
}

// CheckDuplicate finds if there are duplicated items in a list.
func CheckDuplicate(list []string) []string {
	tmpMap := make(map[string]struct{})
	var dupList []string
	for _, name := range list {
		if _, ok := tmpMap[name]; ok {
			dupList = append(dupList, name)
		} else {
			tmpMap[name] = struct{}{}
		}
	}
	return dupList
}
