from fuzzywuzzy import fuzz

with open('/Users/eabis/Downloads/03-13-2017000','r') as in_file:
    with open('/Users/eabis/Documents/dumps/pyspark_tf-idf_text_mining_machine_learning-name-similarity-scores.txt', 'w') as out_file:
        for line in in_file:
            raw_list = line.split("|")
            stripped = list(map(lambda word: word.strip(), raw_list))
            stripped.append(str(fuzz.ratio(stripped[3], stripped[4])))
            stripped.append(str(fuzz.ratio(stripped[5], stripped[6])))
            final_text = "|".join(stripped)
            out_file.write(final_text)
            out_file.write('\n')