↑ [[Concurrency]] · [[Channels]]

# Fan-in / Fan-out

![[Pasted image 20260908231206.png]]

В конвейерах Go эти слова значат конкретное:

- **Fan-out** — несколько горутин читают один канал. Каждое значение забирает кто-то один, пока канал не закроют.
- **Fan-in** — несколько каналов сливаются в один. Читатель наружу видит одну ленту.

Код ниже — эта пара: `fanOut` поднимает `num` воркеров над общим `in`, у каждого свой выход, `fanIn` склеивает выходы.

```go
package main

import (
	"context"
	"fmt"
	"sync"
)

func fanIn(ctx context.Context, chans []chan int) chan int {
	out := make(chan int)
	wg := &sync.WaitGroup{}
	for _, ch := range chans {
		wg.Add(1)
		go func () {
			defer wg.Done()
			for {
				select {
				case val, ok := <-ch:
					if !ok {
						return
					}
					select {
					case out <- val:
					case <-ctx.Done():
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func ()  {
		wg.Wait()
		close(out)
	}()
	return out
}

func fanOut(ctx context.Context, in chan int, num int, f func(int) int) []chan int {
	outPool := make([]chan int, num)
	
	for i := range num {
		select {
		case <-ctx.Done():
			return []chan int{}
		default:
			outPool[i] = pipeline(in, f)
		}
	}
	

	return outPool
}

func pipeline(in chan int, f func(int) int) chan int {
	out := make(chan int)
	go func () {
		for v := range in {
			out <- f(v)
		}
		close(out)
	}()

	return out
}

func operation(val int) int {
	return val * val
}

const (
	numWorkers = 10
)


func main() {
	in := make(chan int)
	go func () {
		for i := range 10 {
			in <- i
		}
		close(in)
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	for v := range fanIn(ctx, fanOut(ctx, in, numWorkers, operation)) {
		fmt.Println(v)
	}

}
```

> [!WARNING] Свой вывод
> Worker pool лучше fan-in / fan-out.

Fan-out здесь заводит 10 промежуточных каналов, fan-in заводит ещё горутину на каждый из них. Worker pool делает ту же работу: `N` горутин читают один `in` и пишут в один `out`. Промежуточных каналов нет, отдельное слияние не нужно.

Что в коде выше всплывает на собеседовании:

- `pipeline` не смотрит на `ctx`. Если потребитель перестал читать, горутина навсегда блокируется на `out <- f(v)`.
- `fanOut` при уже отменённом контексте возвращает пустой слайс и бросает те `pipeline`, которые успел запустить.
- `select` не отдаёт приоритет `ctx.Done()`. Если готовы и значение, и отмена, рантайм выбирает ветку случайно.
- Закрывает вход одна горутина в `main`. Это правильно: `close` делает отправитель, и только один.

## Эталон: worker pool

Один вход, один выход, фиксированное число воркеров. Отмена — через `ctx`, конец данных — через `close(in)`.

```go
func workerPool(ctx context.Context, in <-chan int, workers int, f func(int) int) <-chan int {
	out := make(chan int)
	var wg sync.WaitGroup
	wg.Add(workers)

	for range workers {
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case v, ok := <-in:
					if !ok {
						return
					}
					select {
					case <-ctx.Done():
						return
					case out <- f(v):
					}
				}
			}
		}()
	}

	go func() {
		wg.Wait()
		close(out)
	}()
	return out
}
```

`Add` стоит до `go`. `close(out)` — после того как все воркеры вышли, в отдельной горутине, чтобы `workerPool` мог вернуть канал сразу. Внутри `select` отмена не даёт застрять ни на чтении `in`, ни на записи в `out`.
