#mapper.py
#!/usr/bin/env python3

import sys

def mapper():
    for line in sys.stdin:
        # Split the input line into movie_id and rating
        movie_id, rating = line.strip().split(',')

        try:
            rating = float(rating)
        except ValueError:
            # If the rating is not a valid float, skip this line
            continue

        # Emit key-value pair: movie_id, rating
        print(f"{movie_id}\t{rating}")

if __name__ == "__main__":
    mapper()


#reducer.py
#!/usr/bin/env python3 

import sys

def reducer():
    current_movie_id = None
    total_rating = 0
    num_ratings = 0
    highest_average_rating = float('-inf')
    highest_rated_movie_id = None

    for line in sys.stdin:
        movie_id, rating = line.strip().split('\t')
        rating = float(rating)

        if current_movie_id is None:
            current_movie_id = movie_id

        if movie_id != current_movie_id:
            # Calculate the average rating for the current movie_id
            average_rating = total_rating / num_ratings if num_ratings > 0 else 0

            # Check if the current movie has the highest average rating so far
            if average_rating > highest_average_rating:
                highest_average_rating = average_rating
                highest_rated_movie_id = current_movie_id

            # Reset the total_rating and num_ratings for the next movie_id
            current_movie_id = movie_id
            total_rating = rating
            num_ratings = 1
        else:
            total_rating += rating
            num_ratings += 1

    # Calculate the average rating for the last movie_id
    average_rating = total_rating / num_ratings if num_ratings > 0 else 0

    # Check if the last movie has the highest average rating
    if average_rating > highest_average_rating:
        highest_average_rating = average_rating
        highest_rated_movie_id = current_movie_id

    # Print the movie_id with the highest average rating
    print(f"Highest Rated Movie ID: {highest_rated_movie_id}")
    print(f"Highest Average Rating: {highest_average_rating}")

if __name__ == "__main__":
    reducer()


#mapred streaming
time mapred streaming -files mapper.py,reducer.py -input ratings.csv -output result1 -mapper "python3 mapper.py" -reducer "python3 reducer.py"

#Run MapReduce on single instance 
time cat ratings.csv | python3 mapper.py | sort - | python3 reducer.py > output local.txt
