import numpy as np


def game_core_v3(number):
    """ Устанавливаем упорядоченный список из диапазона чисел,в который входит загаданное число.
        В цикле делаем срезы списка по его левой или правой половине,в зависимости от того,больше
        или меньше загаданное число числа,которое в центре списка.
        Функция принимает загаданное число и возвращает число попыток"""
    count = 1

    number_list = [x for x in range(1, 101)]
    number_list.sort()

    low_index = 0
    mid_index = len(number_list) // 2
    high_index = len(number_list) - 1

    while number_list[mid_index] != number:
        count += 1
        if number > number_list[mid_index]:
            # На случай,если загадонное число последний элемент списка
            if high_index == 1 and number_list[high_index] == number:
                break
            else:
                number_list = number_list[mid_index:high_index + 1]
        else:
            number_list = number_list[low_index:mid_index + 1]

        high_index = len(number_list) - 1
        mid_index = high_index // 2

    return count  # выход из цикла, если угадали


def score_game(game_core):
    '''Запускаем игру 1000 раз, чтобы узнать, как быстро игра угадывает число'''
    count_ls = []
    np.random.seed(1)  # фиксируем RANDOM SEED, чтобы ваш эксперимент был воспроизводим!
    random_array = np.random.randint(1, 101, size=(1000))
    for number in random_array:
        count_ls.append(game_core(number))
    score = int(np.mean(count_ls))
    print(f"Ваш алгоритм угадывает число в среднем за {score} попыток")
    return (score)


score_game(game_core_v3)
