# Детектор правонарушителей

## Принцип работы

Программа получает на вход Excel таблицу, содержащую, как минимум, ИНН и название организации.\
Далее наша программа обрабатывает исходные данные, делая по 30 запросов к нашему серверу.\
Результатом программы является Excel таблица с необходимыми столбцами (*Подробнее смотрите в примере*)

## Пример работы

* ### На вход подаётся Excel таблица. В данном случае такого вида:

    ![Input excel table](https://i.imgur.com/8aHIszG.png "Input excel table")

* ### На самом деле, нам потребуется только эта часть:

  ![Minimum info excel table](https://i.imgur.com/PSxiEOc.png "Min info excel table")

* ### На выходе вы получаете также Excel таблицу, но уже в таком формате:

    ![Handled excel table](https://i.imgur.com/C1Cc2jz.png "Handled excel table")

> Так как это государственные организации, то нарушений среди них будет мало, однако у коммерческих их наблюдается больше.
