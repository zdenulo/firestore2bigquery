"""Script to generate sample data for a Fitness tracking app"""

import os
import datetime

from faker import Faker
from faker.providers import profile
from faker.providers import date_time

from google.cloud import firestore

GCP_PROJECT = os.environ['GCP_PROJECT']

db = firestore.Client(project=GCP_PROJECT)

fake = Faker()
fake.add_provider(profile)
fake.add_provider(date_time)

no_users = 500

exercises = [
    'Lunges', 'Pushups', 'Squats', 'Burpees', 'Planks', 'Side planks', 'Situps', 'Glute bridge', 'Leg press',
    'Deadlift', 'Leg extension', 'Leg curl', 'Bench press', 'Crunches', 'Air bike', 'Ankle circles', 'Arm circles',
    'Boat pose', 'Bodyweight lunge', 'Bow pose', 'Bridge', 'Butts ups', 'Camel pose', 'Cobra', 'Cross body crunch',
    'Cross legged twist', 'Dorsiflexion', 'Elbow circles', 'Flutter kick', 'Knee circles', 'Leg lift']


def create_entry():
    user_exercises = fake.random_sample(elements=exercises,
                                        length=fake.random_int(min=2, max=int(len(exercises) * 0.75)))
    start_datetime = fake.date_time_this_decade()
    exercise_time = fake.random_int(min=10, max=90)
    end_datetime = start_datetime + datetime.timedelta(minutes=exercise_time)
    return {
        'exercises': user_exercises,
        'start_datetime': start_datetime,
        'end_datetime': end_datetime,
        'exercise_time': exercise_time
    }


if __name__ == '__main__':

    for _ in range(no_users):
        user_profile = fake.simple_profile()
        user_profile['birthdate'] = datetime.datetime.combine(user_profile['birthdate'], datetime.time.min)
        print(user_profile)
        user_key = user_profile.pop('mail')
        user_ref = db.collection('users').document(user_key)
        no_exercises = fake.random_int(min=10, max=50)
        user_ref.set(user_profile)
        for _ in range(no_exercises):
            journal = create_entry()
            user_ref.collection('journals').add(journal)
