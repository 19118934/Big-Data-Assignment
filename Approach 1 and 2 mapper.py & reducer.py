#Approach 1
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



#Approach 2
#mapper.py
#!/usr/bin/env python3

import sys

def mapper():
    for line in sys.stdin:
        # Split the input line into userId, movieId, rating, and timestamp
        userId, movieId, rating, _ = line.strip().split(',')

        try:
            rating = float(rating)
        except ValueError:
            # If the rating is not a valid float, skip this line
            continue

        # Emit key-value pair: userId, (movieId, rating)
        print(f"{userId}\t{movieId}\t{rating}")

if __name__ == "__main__":
    mapper()


#reducer.py
#!/usr/bin/env python3

import sys
from collections import defaultdict

def reducer():
    user_ratings = defaultdict(list)

    for line in sys.stdin:
        userId, movieId, rating = line.strip().split('\t')
        rating = float(rating)

        # Collect all ratings given by each user
        user_ratings[userId].append((movieId, rating))

    # User similarity calculation and recommendation generation
    for user, ratings in user_ratings.items():
        # Calculate the average rating for the user
        total_rating = sum(r for _, r in ratings)
        average_rating = total_rating / len(ratings)

        # Generate recommendations for the user
        recommendations = {}
        for movie, rating in ratings:
            for other_user, other_ratings in user_ratings.items():
                if other_user == user:
                    continue

                # Calculate similarity between the current user and other users using Pearson correlation coefficient
                common_movies = set(r[0] for r in ratings) & set(r[0] for r in other_ratings)
                if len(common_movies) == 0:
                    continue

                user_ratings_mean = sum(r for _, r in ratings) / len(ratings)
                other_ratings_mean = sum(r for _, r in other_ratings) / len(other_ratings)

                numerator = sum((r - user_ratings_mean) * (other_rating - other_ratings_mean)
                                for m, r in ratings if m in common_movies
                                for other_movie, other_rating in other_ratings if other_movie == m)

                user_denom = sum((r - user_ratings_mean)**2 for m, r in ratings if m in common_movies)
                other_denom = sum((r - other_ratings_mean)**2 for m, r in other_ratings if m in common_movies)

                if user_denom == 0 or other_denom == 0:
                    continue

                similarity = numerator / (user_denom**0.5 * other_denom**0.5)

                # Make recommendations for movies not yet rated by the user
                for other_movie, other_rating in other_ratings:
                    if other_movie not in user_ratings:
                        recommendations.setdefault(other_movie, []).append((other_rating, similarity))

        # Calculate the final recommendation score for each movie
        final_recommendations = {}
        for movie, values in recommendations.items():
            rating_sum = sum(r * s for r, s in values)
            similarity_sum = sum(abs(s) for _, s in values)
            final_recommendations[movie] = rating_sum / similarity_sum

        # Sort and print the recommendations for the user
        sorted_recommendations = sorted(final_recommendations.items(), key=lambda x: x[1], reverse=True)
        top_recommendations = sorted_recommendations[:10]  # Top 10 movie recommendations
        for movie, score in top_recommendations:
            print(f"{user}\t{movie}\t{score}\t{average_rating}")

if __name__ == "__main__":
    reducer()

#mapred streaming
time mapred streaming -files mapper.py,reducer.py -input ratings.csv -output result2 -mapper "python3 mapper.py" -reducer "python3 reducer.py"
