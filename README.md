# Проект 3

Валентин, посторался поправить твои замечания), есть некоторые вопросы:
1. Как с помошью Postgres оператора отлавить ошибку или успех, чтобы выполнить логированние.
2. Можешь пример с try exept
3. Про индексы, как поступить в данном случае, предположим отдельный таск к конце их создаст после того как все загрузиться, но на сколько это уместно в случае инкрементальной закрузки, когда каждый день приходят новые данные, тогда нужно каждый разу их удалять и проставлять

# # Старое
 Привет)
 
 Здесь лежит мой проект по 3 спринту, надеюсь все будет хорошо и все запустится.
 По некотором не зависящим от меня причинам мне пришлось немного по своему интерпретировать то задание которое было указано на платформе.
 В целом все пункты сделаны, мне пришлось отказаться от некоторых полей в f_customer_retention, так как в исходных  csv которые я скачивал нет нужных полей по которым можнобыло индифицировать заказ как shipped или refunded.
 
 В моем случае f_customer_retention это срез по неделям с показателями 
 new_customers_count, returning_customers_count, new_customers_revenue, returning_customers_revenue без разбивки на category_id, так как тоже не получится индефициорвать пользователей.
 
 Файлы исходники я сложил в  /stage.
 И также немного для своего удобства пересоздавал схемы mart и staging
 
 Порядок работы с проектом такой:
 - create_schemas - создает или пересоздает схемы, запускается в первую очередь
 - upload_once - первоначальная загрузка
 - upload_inc - инкрементальная загрузка, в даге указано что будет выполняться ежедневно. Также есть backfill для пересчетов.
 - Да и есть логи для отслеживания того что записывается и ошибок, staging.events_log - все что связано со staging, и соответственно mart.load_history как еще в теории было.

Ну в общем общение начинается, пиши замечания))
 
 
