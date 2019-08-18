def levenshtein_distance(word1, word2):
    """
    https://medium.com/@yash_agarwal2/soundex-and-levenshtein-distance-in-python-8b4b56542e9e
    https://en.wikipedia.org/wiki/Levenshtein_distance
    :param word1:
    :param word2:
    :return:
    """
    try:
        if (word1 is None) or (word2 is None):
            return 0.0

        word2 = word2.lower()
        word1 = word1.lower()
        matrix = [[0 for x in range(len(word2) + 1)] for x in range(len(word1) + 1)]

        for x in range(len(word1) + 1):
            matrix[x][0] = x
        for y in range(len(word2) + 1):
            matrix[0][y] = y

        for x in range(1, len(word1) + 1):
            for y in range(1, len(word2) + 1):
                if word1[x - 1] == word2[y - 1]:
                    matrix[x][y] = min(
                        matrix[x - 1][y] + 1,
                        matrix[x - 1][y - 1],
                        matrix[x][y - 1] + 1
                    )
                else:
                    matrix[x][y] = min(
                        matrix[x - 1][y] + 1,
                        matrix[x - 1][y - 1] + 1,
                        matrix[x][y - 1] + 1
                    )

        distance = matrix[len(word1)][len(word2)]
        max_ls = max([len(word1), len(word2)])
        if max_ls != 0:
            similarity = round(1-(float(distance)/max_ls), 4)*100
        else:
            similarity = 0.0
    except:
        return 0.0

    return similarity
