[[Network]]

-
# Nginx: Архитектура, Reverse Proxy, Балансировка и Статика

## 1. Архитектура Nginx: почему он такой быстрый?
В отличие от классических серверов (вроде Apache mpm_prefork), создающих отдельный поток/процесс на каждый коннект, Nginx использует **асинхронную событийно-ориентированную модель (Event-driven)**:
- **Master Process:** читает конфиг, связывает порты, управляет worker-процессами.
- **Worker Processes:** обычно запускаются по 1 на ядро CPU. Каждый worker крутит цикл событий (Event Loop) через системный вызов `epoll` (Linux) или `kqueue` (FreeBSD).
- **Результат:** 1 процесс может удерживать десятки тысяч одновременных соединений (C10K problem решена) с минимальным потреблением RAM.

---

## 2. Ключевые функции

### А. Раздача статических файлов (Zero-Copy)
Nginx читает файлы с диска и отправляет в сеть без копирования в адресное пространство приложения:
- Использует системный вызов `sendfile()`. Данные идут напрямую: **Диск → Буфер ядра (Page Cache) → Сетевой сокет**.
- Директива `tcp_nopush on;` заставляет накапливать пакеты и отправлять их полным размером (MSS), экономя сетевые оклады.

```nginx
server {
    listen 80;
    server_name static.example.com;

    root /var/www/assets;

    location / {
        # Сначала ищет файл по пути, потом папку, если нет — 404
        try_files $uri $uri/ =404;
        
        # Кеширование в браузере на 30 дней
        expires 30d;
        add_header Cache-Control "public, no-transform";
    }
}
```

### Б. Reverse Proxy (Обратный прокси)
Nginx принимает запрос от клиента и перенаправляет его на бэкенд (Go, Python, Node.js), защищая бэкенд от сырого внешнего мира.

> [!important] Важно прокидывать заголовки!
> Без прокидывания заголовков ваш Go-бэкенд будет думать, что все клиенты подключаются с IP `127.0.0.1`.

```nginx
server {
    listen 80;
    server_name api.example.com;

    location /api/ {
        proxy_pass http://127.0.0.1:8080; # Адрес нашего Go-приложения
        
        # Передача метаданных реального клиента
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;

        # Буферизация: собирает медленный запрос целиком, спасая бэкенд от зависаний
        proxy_buffering on;
    }
}
```

---

### В. Балансировка нагрузки (Load Balancing)
Задается через блок `upstream`. Nginx распределяет трафик между репликами приложения.

```nginx
upstream backend_cluster {
    # Алгоритмы по умолчанию: Round Robin (по кругу)
    # least_conn; # отправлять тому, у кого меньше активных соединений
    # ip_hash;    # привязка сессии: один IP всегда летит на один и тот же бэкенд

    server 10.0.0.1:8080 weight=3; # weight=3 получит в 3 раза больше запросов
    server 10.0.0.2:8080 max_fails=3 fail_timeout=10s;
    server 10.0.0.3:8080 backup;   # включится, только если упадут первые два
}

server {
    listen 80;
    server_name app.example.com;

    location / {
        proxy_pass http://backend_cluster;
    }
}
```

---

### Г. Сжатие Gzip
Снижает размер передаваемых JSON/HTML/CSS/JS на 60–80%, ускоряя загрузку у пользователей с плохим интернетом.

> [!warning] Что НЕ надо сжимать:
> Картинки (JPEG, PNG), видео (MP4), архивы (ZIP) — они уже сжаты. Попытка их сжать только сожрет CPU сервера впустую.

```nginx
gzip on;
gzip_comp_level 5; # Оптимально 4-6 (выше — жрет много CPU при минимальном выигрыше)
gzip_min_length 256; # Файлы меньше 256 байт сжимать не имеет смысла
gzip_proxied any; # Сжимать даже ответы для проксированных запросов
gzip_types
    text/plain
    text/css
    application/json
    application/javascript
    application/x-javascript
    text/xml
    application/xml
    image/svg+xml;
```
```
