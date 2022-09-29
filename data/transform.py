import os
import sys

directory = './'

for filename in os.scandir(directory):
    if filename.is_file():
        new_data = ""
        title = ""
        author = ""
        date = ""
        delimeter = "<~~~>"
        in_header = True
        valid_header = True
        with open(filename) as fr:
            try:
                for line in fr:
                    if len(title) > 0 and in_header:
                        if line == "*** START OF THIS PROJECT GUTENBERG EBOOK " + title.upper() + " ***\n":
                            in_header=False
                            if len(title) == 0 or len(author) == 0 or len(date) == 0:
                                print("PARSE ERROR: invaid header in " + filename)
                                valid_header = False
                                break
                            continue
                            
                    if in_header:
                        words = line.split()
                        for i in range(len(words)):
                            if words[i] == "Title:" and len(title)==0:
                                title = " ".join(words[1:])
                            if words[i] == "Author:" and len(author) == 0:
                                author = " ".join(words[1:])
                                new_data += (author + delimeter)
                            if words[i] == "Release" and words[i+1] == "Date:" and len(date) == 0:
                                #print(filename.name + " : " + str(words))
                                if "[EBook" in words or "[Etext" in words:
                                    date = words[-3]
                                else:
                                    date = words[-1]
                                if len(date) == 0: raise Exception("Unsopported Date Format")
                                new_data += (date + delimeter)
                    if not in_header:
                        if line == "*** END OF THIS PROJECT GUTENBERG EBOOK " + title.upper() + " ***\n":
                            break
                        new_data += line
            except (UnicodeDecodeError, IndexError, Exception) as e:
                print(e)
                print("Invalid format in: " + filename.name)
                os.remove(filename)
                continue      
        if valid_header:                
            with open(filename, "w") as fw:
                fw.write(new_data)
