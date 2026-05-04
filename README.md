# Отчетики

MVP автоматизированных отчетов:

- подтягивает загруженные лиды из Битрикса;
- считает показатели по всей загруженной базе;
- внутренне считает "новую базу" как первое появление телефона внутри конкретного первичного `utm_content`, а не первое появление телефона в CRM вообще;
- считает номер круга по возвратам лида в стадию `Необработанное`;
- отдельно выводит лиды в процессе обработки и лиды в доработке;
- группирует выгрузки по дате из `utm_term`, если там указана дата;
- не скрывает историю прошлых месяцев при переходе на новый месяц: в Google Sheets лист `Показатели` строится по накопленной истории из локальной базы, начиная с `2026-05-01`;
- в Google Sheets на листе `Показатели` данные можно сворачивать и разворачивать по месяцам и по отдельным дням через встроенные группы строк;
- лист `Итог по источникам` вынесен отдельно от детализации, поэтому его не затрагивает сворачивание строк в `Показатели`;
- автоматическое обновление отчета по базам пересинхронизирует Битрикс за период `2026-05-01`..`today`, чтобы при сбоях расписания не оставались дыры в конце месяца;
- не считает в объем загрузки лиды без источника Битрикса, чтобы не подмешивать ручные дубли с теми же UTM;
- подтягивает текущие статусы лидов из Битрикса;
- подтягивает звонки из Скорозвона за отчетный период;
- формирует CSV-отчеты в папке `reports/`.

## Что такое upload_id

`upload_id` нужен только для технического режима импорта CSV. В рабочих отчетах для пользователей он скрыт.

Пример:

```text
2026-04-14_d2_plat_tmn_phone_Tyumen_001
```

Если импортировать тот же файл повторно, скрипт переиспользует тот же `upload_id` и не задваивает базу.

Если тот же файл нужно зарегистрировать как реальный новый перезалив, используйте флаг:

```powershell
--force-new-upload
```

## Запуск

```powershell
npm run report:auto:sheets
```

Команда берет текущий месяц с 1 числа по сегодня, подтягивает Битрикс, Скорозвон и обновляет Google Sheets.

Загрузить фиксированный период:

```powershell
npm run report -- --source bitrix --from 2026-04-01 --to 2026-04-14 --google-sheets
```

Для текущего апреля:

```powershell
npm run report:april:sheets
```

Файлы из `Examples/` используются только как референсы и тестовые примеры. Рабочие отчеты строятся из Битрикса и Скорозвона.

## Результаты

Скрипт создает:

- `reports/base_report.csv` - отчет по базам;
- `reports/callability_daily.csv` - дневная дозваниваемость;
- `reports/callability_by_base.csv` - дозваниваемость по первой базе номера;
- `reports/upload_items.csv` - детализация строк загрузки.

Локальная база состояния хранится в `data/reporting-db.json`.

## Автообновление

### Облачное расписание

Для автономного обновления без привязки к компьютеру добавлен GitHub Actions workflow:

```text
.github/workflows/update-reports.yml
```

Он запускается:

- каждые 2 часа;
- вручную через `workflow_dispatch`.

Каждый запуск сначала обновляет Битрикс за текущий месяц и сразу записывает отчет по базам в Google Sheets. После этого отдельным шагом обновляется Скорозвон: при первом запуске за текущий месяц, при следующих запусках только за сегодняшний день, чтобы не упираться в rate limit. Если Скорозвон временно не ответит, отчет по базам все равно останется обновленным.

Номер круга считается по истории стадий Битрикса: первый вход лида в `Необработанное` считается первым кругом, каждый следующий возврат из другой стадии в `Необработанное` увеличивает круг на 1.

Локальная база отчета хранится в GitHub Actions cache, а не на компьютере.

В GitHub нужно добавить repository secrets:

```text
BITRIX_WEBHOOK_URL
SKOROZVON_USERNAME
SKOROZVON_API_KEY
SKOROZVON_CLIENT_ID
SKOROZVON_CLIENT_SECRET
GOOGLE_SERVICE_ACCOUNT_JSON
GOOGLE_SPREADSHEET_ID
```

`GOOGLE_SERVICE_ACCOUNT_JSON` - содержимое JSON-ключа service account целиком.

Если установлен GitHub CLI, secrets можно загрузить из локальных `bitrix.env`, `skorozvon.env`, `google.env` и `credentials/` одной командой:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\set-github-secrets.ps1 -Repo owner/repo
```

Перед этим нужно выполнить:

```powershell
gh auth login
```

Если у репозитория настроен `origin` на GitHub, параметр `-Repo owner/repo` можно не указывать.

Опционально можно добавить repository variable:

```text
GOOGLE_SPREADSHEET_TITLE
```

### Локальное расписание

На Windows задача планировщика называется:

```text
OtchetikiReportsEvery2Hours
```

Она запускает:

```powershell
npm run report:2h:sheets
```

Логика каждые 2 часа:

- Битрикс обновляется за текущий месяц с 1 числа по сегодня;
- Скорозвон обновляется только за сегодняшний день;
- Google Sheets обновляется автоматически;
- логи пишутся в `logs/`.

Проверить статус:

```powershell
Get-ScheduledTask -TaskName OtchetikiReportsEvery2Hours
Get-ScheduledTaskInfo -TaskName OtchetikiReportsEvery2Hours
```

Переустановить задачу:

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\scripts\install-scheduled-task.ps1
```

Отключить:

```powershell
Disable-ScheduledTask -TaskName OtchetikiReportsEvery2Hours
```

## Google Sheets

Отчеты можно выгрузить в один Google Spreadsheet с видимыми листами:

- `Показатели`;
- `Дозваниваемость`;
- `Методика`.

Технические листы с `upload_id`, детализацией строк и полными данными дозвона создаются скрытыми.

Поддерживается service account.

1. В Google Cloud включите Google Sheets API и Google Drive API.
2. Создайте service account и JSON-ключ.
3. Положите ключ, например, в `credentials/google-service-account.json`.
4. Создайте `google.env` по примеру `google.env.example`.

Вариант 1: обновлять существующую таблицу.

```env
GOOGLE_SERVICE_ACCOUNT_JSON=credentials/google-service-account.json
GOOGLE_SPREADSHEET_ID=spreadsheet-id-from-url
GOOGLE_SPREADSHEET_TITLE=Отчетики
```

Таблицу нужно расшарить на email service account с правами редактора.

Вариант 2: создать новую таблицу автоматически.

```env
GOOGLE_SERVICE_ACCOUNT_JSON=credentials/google-service-account.json
GOOGLE_SPREADSHEET_TITLE=Отчетики
GOOGLE_SHARE_EMAIL=your-email@example.com
```

Запуск с выгрузкой в Google Sheets:

```powershell
npm run report:auto:sheets
```

Повторно отправить уже собранные данные без синхронизации Битрикса и Скорозвона:

```powershell
npm run sheets
```
