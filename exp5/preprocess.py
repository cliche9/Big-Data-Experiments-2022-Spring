import imdb
import csv

access = imdb.IMDb()

with open('dataset/movies.csv') as r:
    movie_rows = csv.reader(r)
    l = open('dataset/links.csv')
    link_rows = csv.reader(l)
    with open('dataset/movies_out.csv', 'w') as w:
        movie_writer = csv.writer(w)
        
        head = next(movie_rows)
        next(link_rows)
        head.append('img')
        movie_writer.writerow(head)
        
        for row in movie_rows:
            link_row = next(link_rows)
            movie = access.get_movie(int(link_row[1]))
            print(movie['cover url'])
            row.append(movie['cover url'])
            movie_writer.writerow(row)
    l.close()