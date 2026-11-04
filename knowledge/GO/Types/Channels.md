

↑ [[Concurrency]] · [[Buffered channels]] · [[Race]] · [[scheduler]] · [[Fan-in Fan-out]]

# Часть 1. Основы каналов

## Небуферизированные каналы

![[Pasted image 20260906164903.png]]

Канал — это концепция, которая включает в себя:
- **Разделение на роли:** читатели и писатели.
- **Любое количество участников** в рамках одной роли.
- **Конкурентно-безопасная передача данных** от писателя к читателю («из рук в руки»).
- **Блокирующее ожидание** при отсутствии второй стороны.
- **Очереди ожидания** для читателей и писателей (`recvq` и `sendq`).

В рантайме это структура `hchan`. В ней кольцевой буфер (`buf`, индексы `sendx` / `recvx`, счётчик `qcount`, ёмкость `dataqsiz`), мьютекс на сам канал и две очереди `sudog`. В `sudog` лежат горутина и слот под элемент. У небуферизированного канала `dataqsiz` равен 0.

Отправка под замком канала идёт так:

1. Канал закрыт — `panic`.
2. В `recvq` кто-то спит — элемент копируется ему напрямую, горутина просыпается. Буфер не используется. Так устроено рандеву.
3. Иначе, если в буфере есть место, элемент кладётся в `buf`.
4. Иначе отправитель паркуется в `sendq`.

Спящий получатель бывает только при пустом буфере, спящий отправитель — только при полном. Чтение симметрично. Парковка здесь — это сон горутины, не системный вызов: поток ОС остаётся свободен, см. [[scheduler]].

---

## Буферизированные каналы

![[Pasted image 20260906165140.png]]

- **Применение:** Rate limiter, worker pool, семафоры и т.д. (концепции у них похожи).
- Каналы с буфером и без буфера **не взаимозаменяемы**.
- **Проблема с буфером:** данные могут остаться в буфере, если консьюмеры аварийно завершились («отвалились»).

---

## Итог

* **Обычный (небуферизированный) канал** используется, когда передача данных между горутинами — это конечная цель коммуникации.
* **Канал с буфером** используется, когда горутины передают друг другу служебную информацию об определенном ограничении (семафор, рейт-лимит).
* Использование буферизированного канала *только* для передачи данных возможно, но создает лишний оверхед: придется более внимательно писать код, учитывать возможные потери данных и т.д.
* Если без буфера для потоковой передачи данных точно не обойтись — лучше использовать готовые решения (например, Kafka).
* **Канал** — это монотонная передача данных из одной горутины в другую. Успешно отправленные значения читаются в том же порядке (FIFO). С каждой стороны горутин может быть несколько; между разными отправителями порядок — это порядок, в котором они захватили канал.

---

# Часть 2. Состояния каналов и поведение

![[Pasted image 20260906170237.png]]

![[Pasted image 20260906170337.png]]

### Таблица 1. Небуферизированный канал (состояния каналов)

| Операция | Открытый | Закрытый | Неинициализированный (`nil`) |
| :--- | :--- | :--- | :--- |
| **Чтение** (`<-ch`) | Блокировка до прихода писателя | Zero value, `ok == false` | Блокировка навсегда |
| **Запись** (`ch <- v`) | Блокировка до прихода читателя | `panic` | Блокировка навсегда |
| **Закрытие** (`close(ch)`) | Канал закроется | `panic` | `panic` |

---

### Таблица 2. Буферизированный канал

| Операция | Открытый и частично заполненный | Открытый и полностью заполненный | Открытый и пустой | Закрытый и частично заполненный |
| :--- | :--- | :--- | :--- | :--- |
| **Чтение** (`<-ch`) | Прочитаем значение | Прочитаем значение | Блокировка до прихода писателя | Прочитаем значение |
| **Запись** (`ch <- v`) | Запишем значение | Блокировка до прихода читателя | Запишем значение | `panic` |

> [!NOTE]
> **Неинициализированный канал** — объявленный через `var ch chan int` без использования `make`. Это относится и к таблице 1.
>
> Закрытый пустой буфер: чтение даёт zero value и `ok == false`, запись — `panic`. `close` уже закрытого или `nil`-канала — тоже `panic`. После `close` буферизированного канала то, что ещё лежит в буфере, читается как обычно, и только потом приходит zero value.
>
> Чтение из закрытого канала не блокируется: приходит zero value и `ok == false`. Цикл `for { v := <-ch }` без проверки `ok` — не дедлок, он крутится вечно. `for v := range ch` из закрытого канала выходит сам.

---

# Практика: Детекция Deadlock

### 1. Дедлок: блокировка на записи в текущей горутине
```go
package main

import "fmt"

func main() {
	ch := make(chan int)

	ch <- 1
	v := <-ch

	fmt.Println(v)
}
```

### 2. Дедлок: блокировка на чтении
```go
package main

import "fmt"

func main() {
	ch := make(chan int)
	go func ()  {
		ch <- 1		
	}()
	v := <-ch

	fmt.Println(v)
	<-ch
}
```

### 3. Дедлок: читаем больше, чем записали
```go
package main

import "fmt"


func writer() chan int {
	ch := make(chan int)
	go func () {
		for i := range 10 {
			ch <- i		
		}
	}()
	
	return ch
}


func main() {
	ch := writer()

	for range 11 {
		v := <- ch

		fmt.Println(v)
	}

}
```

---

# Паттерны работы с каналами

### Синхронизация по закрытию (если неизвестно число записей)
```go
package main

import "fmt"


func writer() chan int {
	ch := make(chan int)
	go func () {
		for i := range 10 {
			ch <- i		
		}
		close(ch)
	}()
	
	return ch
}


func main() {
	ch := writer()

	for {
		v, ok := <- ch
		if !ok {
			break
		}
		fmt.Println(v)
	}

}
```

* **Паттерн «Генератор»:** между созданием канала и возвратом из функции не должно быть блокирующих операций (чтения/записи) — вся работа выполняется в горутине.
* Канал **не обязательно** закрывать всегда (это не файл/дескриптор ОС). Если мы точно знаем количество записей, закрывать не обязательно. Но если мы обходим канал через `for v := range ch`, закрыть его необходимо, иначе будет дедлок.

---

### Задача 1: Пайплайн (Pipeline)

**Условие:** Написать три функции:
- `writer` — генерирует числа от 1 до 10;
- `doubler` — умножает числа на 2;
- `reader` — читает и выводит на экран.
- Связка: `reader(doubler(writer()))`.

```go
package main

import (
	"fmt"
	"time"
)

func writer() <-chan int {
	ch := make(chan int)
	go func() {
		for i := range 10 {
			ch <- i + 1
		}
		close(ch)
	}()
	return ch
}

func doubler(ch <-chan int) <-chan int {
	chd := make(chan int)
	go func() {
		for v := range ch {
			dv := v * 2
			chd <- dv
			time.Sleep(500 * time.Millisecond)
		}
		close(chd)
	}()
	
	return chd
}

func reader(ch <-chan int) {
	for v := range ch {
		fmt.Println(v)
	}
}


func main() {
	reader(doubler(writer()))
}
```

Тот же приём, когда стадий много и выходы надо слить обратно в один канал — [[Fan-in Fan-out]].

---

### Пример: Чтение и запись в один и тот же канал несколькими воркерами
```go
package main

import (
	"fmt"
)


func main() {
	ch := make(chan int)

	go func () {
		for i := range 10000 {
			ch <- i
		}
		close(ch)
	}()

	go func () {
		for i := range 10000 {
			ch <- i * 2
		}
		close(ch)
	}()
	
	go func ()  {
		for v := range ch {
			fmt.Println(v, "worker 2")
		}
	}()

	for v := range ch {
		fmt.Println(v, "worker 1")
	}
}
```

> [!DANGER] Два `close`
> Оба писателя вызывают `close(ch)`. Повторное закрытие — `panic: close of closed channel`. Закрывает канал один владелец, после того как все отправители точно закончили (обычно `WaitGroup`, затем один `close`). Код оставлен как есть: на нём это и всплывает. Чтение несколькими горутинами из одного канала само по себе законно.

---

# Работа с `select`

`select` по умолчанию является блокирующим. Нужно всегда следить за тем, чтобы была ветка, способная его разблокировать.

### 1. Неблокирующий `select` с веткой `default`
```go
package main

import (
	"fmt"
)


func main() {
	ch1 := make(chan int)
	ch2 := make(chan int)

	go func ()  {
		ch2 <- 5
	}()

	select {
	case v := <- ch1:
		fmt.Println("ch1 ", v)
	case v := <- ch2:
		fmt.Println("ch2 ", v)	
	default:
		fmt.Println("default")
	}
}
```

### 2. Мультиплексирование с таймаутами и контекстом
```go
package main

import (
	"context"
	"fmt"
	"time"
)


func main() {
	ch1 := make(chan int)
	ch2 := make(chan int)

	timer := time.NewTimer(1 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	select {
	case v := <- ch1:
		fmt.Println("ch1 ", v)
	case v := <- ch2:
		fmt.Println("ch2 ", v)	
	case <- time.After(1 * time.Second):
		fmt.Println("exited by after")
	case <- timer.C:
		fmt.Println("exited by timer")
	case <- ctx.Done():
		fmt.Println("context")
	}
}
```

---

# Утечки горутин (Goroutine Leaks)

### Пример утечки горутины
Горутина пытается писать в `ch`, но `main` выходит по таймауту контекста и перестает читать из канала. Горутина-писатель навсегда зависает в памяти:
```go
package main

import (
	"context"
	"fmt"
	"time"
)


func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	ch := make(chan int)

	go func() {
		for i := range 1000 {
			ch <- i
		}
		close(ch)
	}()

	for {
		select {
		case v := <-ch:
			fmt.Println(v)
		case <-ctx.Done():
			fmt.Println("context ended")
			return
		}
	}
}
```

### Предотвращение утечки горутин
Писатель слушает `<-ctx.Done()` и прерывает попытки записи:
```go
package main

import (
	"context"
	"fmt"
	"time"
)


func main() {
	ctx, cancel := context.WithTimeout(context.Background(), time.Microsecond)
	defer cancel()

	ch := make(chan int)

	go func() {
		for i := range 1000 {
			select {
			case ch <- i:
			case <-ctx.Done():
			}
		}
		close(ch)
	}()

	for {
		select {
		case v := <-ch:
			fmt.Println(v)
		case <-ctx.Done():
			fmt.Println("context ended")
			return
		}
	}
}
```

Ветка `ctx.Done()` пустая, из цикла писатель сразу не выходит. Утечки заблокированного отправителя уже нет: `select` не даёт навсегда встать на `ch <- i`, и после тысячи итераций горутина доходит до `close` и завершается. Чтобы остановиться на первой же отмене, в этой ветке нужен `return`. Если готовы и отправка, и `ctx.Done()`, `select` выбирает ветку случайно.

---

# Практические задачи

### Задача 2: Обертка с контролем таймаута
Написать обертку для функции, которая прерывает выполнение, если работа длится дольше 3 секунд:
```go
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"
)

func randomTimeWork() {
	time.Sleep(time.Duration(rand.IntN(100)) * time.Second)
}
// написать обертку функции которая будет прерывать выполнение 
// если функция раблотает дольше 3 секунд
func predictableTimeWork() {
	ch := make(chan struct{})
	go func () {
		randomTimeWork()
		close(ch)
	}()

	select {
	case <-ch:
		fmt.Println("func worked under 3 sec")
	case <-time.After(3 * time.Second):
		fmt.Println("func worker above 3 src exiting by context")
	}
}


func main() {
	predictableTimeWork()
}
```

Обёртка перестаёт ждать через 3 секунды и не отменяет `randomTimeWork`: горутина доспит свой `Sleep` уже после выхода из `select`. В печати написано «context», в коде — `time.After`. Для одного ожидания это равносильно по сроку, отдельного `context` здесь нет. `time.After` вызван один раз, не в цикле.

---

### Задача 3: Параллельная обработка с ограничением по времени
**Условие:** функция `processParallel` должна отработать не больше 5 секунд и обработать максимальное количество входящих запросов.

```go
package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

const (
	NUM_IN = 100
)

func processData(val int) int {
	time.Sleep(time.Duration(rand.IntN(10)) * time.Second)
	return val * 2
}


func main() {
	in := make(chan int)
	out := make(chan int)

	go func	() {
		for i := range NUM_IN {
			in <- i
		}
		close(in)
	}()
	now := time.Now()
	processParallel(in, out, 5)

	for val := range out {
		fmt.Println(val)
	}
	fmt.Println(time.Since(now))

}

func processParallel(in <-chan int, out chan<- int, numWorkers int) {
	batchSize := NUM_IN / numWorkers
	ctx, cancel := context.WithTimeout(context.Background(), time.Second * 5)
	wg := &sync.WaitGroup{}
	for range numWorkers {
		wg.Add(1)
		go func ()  {
			defer wg.Done()
			for range batchSize {
				resCh := make(chan int)
				val := <- in
				go func ()  {
					resCh <- processData(val)					
				}()
				select {
				case res := <-resCh:
					select {
					case out <- res:
					case <-ctx.Done():
						return 
					}
				case <-ctx.Done():
					return
				}
				}
		}()
	}
	wg.Wait()
	cancel()
	close(out)
}
```

Что в этом решении всплывает:

- `val := <-in` стоит вне `select`. Пока вход молчит, таймаут контекста на это чтение не действует.
- На каждую задачу запускается горутина с небуферизированным `resCh`. Если раньше сработал `ctx.Done()`, читателя у `resCh` уже нет, и горутина навсегда блокируется на отправке.
- `batchSize := NUM_IN / numWorkers` — это нарезка фиксированными пачками, не общая очередь. Хвост при нецелом делении никто не обработает. `processData` контекст не смотрит, её `Sleep` не прерывается.
- Рабочий вариант той же задачи с общим входом и одним выходом — worker pool в [[Fan-in Fan-out]].