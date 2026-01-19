from collections import defaultdict
import random
from .mariadb import *
from datetime import datetime, timedelta, time, date
import string

FAMILIES = 20
USERS = 100
MEDIA = 100
SERIES = 50
FILMS = 50
DEVICES = 100
SESSIONS = 20
FRIENDSHIPS = 50
WATCHHISTORIES = 100

def generate_random_data() :
    """
    Generates random data. 20 families, 100 users, 100 media, ~50 films, 
    ~50 series, 100 devices, 20 sessions, 50 friendships, 100 watchhistories.
    All foreign keys are created in range 1-n where n is the number of rows in a referenced table.
    It is safe to assume that the ids start with 1, as auto increment keys are reset upon data generation
    for every table.
    """

    print("Generating random data")
    generate_families()
    print("20 Families generated")
    generate_users()
    print("100 Users generated")
    generate_media()
    print("100 Media generated")
    generate_series_films()
    print("100 Series and films generated")
    generate_sessions()
    print("20 Sessions generated")
    generate_watch_histories()
    print("100 Watch Histories generated")
    generate_devices()
    print("100 Devices generated")
    generate_friendships()
    print("50 Friendships generated")

    # Clear empty families
    deleted = execute_delete("""
        DELETE FROM Family
        WHERE family_id NOT IN (
            SELECT DISTINCT family_id
            FROM Users
            WHERE family_id IS NOT NULL
        )
    """, ())
    print(f"{deleted} empty Families deleted")

    print("Data generation successfull")



def random_date(days_range: int) -> date:
    # Generates Random date by substracting days from today's date
    return datetime.now().date() - timedelta(days=random.randint(0, days_range))

def random_time(max_hours: int) -> time:
    # Generates random time in range from 00:00:00 to 23:59:59
    return time(
        hour=random.randrange(0, min(max_hours, 24)),
        minute=random.randrange(0, 60),
        second=random.randrange(0, 60)
    )

def generate_families() :
    family_types = ["Family", "Couple", "Friends", "Corporate"]
    for i in range(FAMILIES) :
        insert_family( Family(None, random.choice(family_types), random_date(365)) )

def generate_users() :
    for i in range(USERS) :
        name = random.choice(names_pool)
        email = name.replace(' ', '')+str(random.randrange(1,1000))+"@gmail.com"
        birthday = random_date(29200)
        location = random.choice(cities_pool)
        bio = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randrange(1, 50)))
        family_id = random.randint(1, FAMILIES)
        insert_user( User(None, name, email, birthday, location, bio, family_id) )

def generate_media() :
    media_names_sample = random.sample(media_names_pool, len(media_names_pool))
    for i in range(MEDIA) :
        media_name = media_names_sample[i]
        genre = random.choice(genre_pool)
        prod_year = random_date(29200).year
        descr = ''.join(random.choices(string.ascii_letters + string.digits, k=random.randrange(1, 50)))
        location = random.choice(countries_pool)
        cost = random.randint(1, 8)
        insert_media( Media(None, media_name, genre, prod_year, descr, location, cost) )

def generate_series_films() :
    media_id_sample = random.sample(range(1, MEDIA+1), MEDIA)
    for i in range(SERIES + FILMS) :
        if bool(random.getrandbits(1)) :
            number_of_episodes = random.randrange(1, 40)
            is_ongoing = bool(random.getrandbits(1))
            media_id = media_id_sample[i]
            insert_series( Series(None, number_of_episodes, is_ongoing, media_id) )
        else :
            duration = random.randint(15, 300)
            number_of_parts = random.randrange(1,11)
            media_id = media_id_sample[i]
            insert_film( Film(None, duration, number_of_parts, media_id) )

def generate_sessions() :
    inserted_sessions: dict[int, list[int]] = defaultdict(list)
    attempts = 0
    while attempts < SESSIONS :
        user_id = random.randint(1, USERS)
        media_id = random.randint(1, MEDIA)
        if media_id in inserted_sessions[user_id] :
            continue
        attempts += 1
        inserted_sessions[user_id].append(media_id)
        date_of_rent = datetime.combine(random_date(10), random_time(24))
        cost = random.randrange(1, 50) # TODO: add calculation
        duration = random.randrange(1, 21)
        insert_session( Session(None, user_id, media_id, date_of_rent, cost, duration) )

def generate_watch_histories() :
    for i in range(WATCHHISTORIES) :
        user_id = random.randint(1, USERS)
        media_id = random.randint(1, MEDIA)
        date_watched = random_date(365)
        family_watch = bool(random.getrandbits(1))
        insert_watch_history( WatchHistory(None, user_id, media_id, date_watched, family_watch) )

def generate_devices() :
    for i in range(DEVICES) :
        device_name = random.choice(device_names_pool)
        registration_date = random_date(365)
        user_id = random.randint(1, USERS)
        insert_device( Device(None, device_name, registration_date, user_id) )

def generate_friendships() :
    inserted_friendships: set[tuple[int, int]] = set()
    attempts = 0
    while attempts < FRIENDSHIPS :
        user_id_1 = random.randint(1, USERS)
        user_id_2 = random.randint(1, USERS)
        if user_id_1 == user_id_2 or (user_id_1, user_id_2) in inserted_friendships or (user_id_2, user_id_1) in inserted_friendships:
            continue
        attempts += 1
        inserted_friendships.add((user_id_1, user_id_2))
        insert_friendship( Friendship(user_id_1, user_id_2) )

names_pool = [
    "Juliana Adams",
    "Hudson James",
    "Quinn Duran",
    "Ismael Meyers",
    "Leyla Deleon",
    "Nasir Dejesus",
    "Julissa Lim",
    "Cal Velasquez",
    "Esme Wilkins",
    "Yusuf Stevens",
    "Katherine Bates",
    "Ellis Harrell",
    "Kara Blevins",
    "Avi Townsend",
    "Azalea Ellison",
    "Kye McKee",
    "Kori Bonilla",
    "Aden Golden",
    "Giuliana Fleming",
    "Fernando Daugherty",
    "Magdalena Knox",
    "Valentin Burch",
    "Freyja Watson",
    "Greyson Arnold",
    "Finley Randall",
    "Trenton Cooper",
    "Serenity Bond",
    "Roger Perry",
    "Clara Baker",
    "Ezra Lozano",
    "Cecelia Rowland",
    "Eliezer Barrett",
    "Kendall Ochoa",
    "Winston Serrano",
    "Allie Harris",
    "Samuel Berger",
    "Laylah Stanton",
    "Zyair Matthews",
    "Lila Gray",
    "Nicholas Huff",
    "Karsyn Graves",
    "Cesar Ali",
    "Zelda Pitts",
    "Trey Mays",
    "Denisse Briggs",
    "Case Person",
    "Dylan Bailey",
    "Axel Cohen",
    "Destiny Pollard",
    "Jad Huber",
    "Raquel Harvey",
    "Cayden Hester",
    "Zendaya Charles",
    "Conrad Calhoun",
    "Thalia Michael",
    "Bronson Garner",
    "Jacqueline Ho",
    "Morgan Brock",
    "Jada Nielsen",
    "Tru Finley",
    "Jovie Sheppard",
    "Trent Bell",
    "Melody Cook",
    "Ezekiel Robbins",
    "Stevie Mullins",
    "Allen Reid",
    "Charlee Floyd",
    "Pierce Douglas",
    "Aniyah McLean",
    "Crosby Gardner",
    "Jordyn Gray",
    "Nicholas Castro",
    "Eloise Dalton",
    "Fletcher Meyer",
    "Sara Curry",
    "Briggs Bean",
    "Jenesis Huang",
    "Ayaan Michael",
    "Aubriella Burgess",
    "Kolton Calderon",
    "Serena Wolfe",
    "Donovan McFarland",
    "Annika Hogan",
    "Sonny Travis",
    "Mazikee Cisneros",
    "Alden Flores",
    "Emilia Daniels",
    "Xander Gallagher",
    "Elliott Solis",
    "Ronin Dickson",
    "Emmalynn Roy",
    "Marcelo Henderson",
    "Maria Arellano",
    "Kellan Warner",
    "Wynter Nicholson",
    "Rodrigo Wallace",
    "Arianna Owen",
    "Cannon Woods",
    "Reese Summers",
    "Darius Dickerson"
]

cities_pool = [
    "Nata",
    "Jerusalem",
    "Nukualofa",
    "San Pedro Sula",
    "Lake Sevan",
    "Tsuruoka",
    "Islamabad",
    "Esperance",
    "Purnululu NP",
    "Prague",
    "Plymouth",
    "Kobe",
    "Braga",
    "Porirua",
    "Algiers",
    "Hallstatt",
    "Glenrothes",
    "Ushuaia",
    "Kissama NP",
    "Noosa",
    "Sangsong",
    "Palapye",
    "Belfast",
    "Codrington",
    "Villach",
    "Dumfries",
    "Greenock",
    "Hamburg",
    "Moscow",
    "Vienna"
]

media_names_pool = [
    "Detachment",
    "Horrible Bosses 2",
    "Death of a Salesman",
    "Intermission",
    "October Sky",
    "Blackfish",
    "Hotel Transylvania",
    "Moon",
    "Les Choristes",
    "Interstellar",
    "The Strangers",
    "The Godfather",
    "Pulp Fiction",
    "The Fault in Our Stars",
    "Ferris Bueller's Day Off",
    "Primal Fear",
    "American Beauty",
    "Crash",
    "Chronicle",
    "The Spiderwick Chronicles",
    "Andre",
    "Edge of Tomorrow",
    "127 Hours",
    "Dawn of the Planet of the Apes",
    "Nurse Betty",
    "Bridget Jones's Diary",
    "Turbo",
    "Ocean's Eleven",
    "Sliding Doors",
    "Pleasantville",
    "Jurassic Park",
    "The Mummy",
    "The Fantastic Four",
    "Spider-Man",
    "Spider-Man 2",
    "The Day After Tomorrow",
    "I, Robot",
    "Mr",
    "Iron Man",
    "Spider-Man 3",
    "Changeling",
    "2012",
    "Scent of a Woman",
    "Lucy",
    "Argo",
    "Step Up",
    "A Million Ways to Die in the West",
    "Ted",
    "My Girl",
    "Maleficent",
    "22 Jump Street",
    "Boyhood",
    "Little Miss Sunshine",
    "Billy Elliot",
    "In the Name of the Father",
    "The Purge",
    "The Spectacular Now",
    "The Aviator",
    "Pretty in Pink",
    "Road to Perdition",
    "Hancock",
    "Jeff, Who Lives at Home",
    "The Usual Suspects",
    "The Family",
    "The Departed",
    "Bad Neighbours",
    "Calamity Jane",
    "Saving Mr",
    "The Inbetweeners 2",
    "My Dog Skip",
    "The Cabin in the Woods",
    "Paranormal Activity",
    "Monster-In-Law",
    "She's the Man",
    "John Tucker Must Die",
    "Valentine's Day",
    "The House Bunny",
    "Bride Wars",
    "Confessions of a Shopaholic",
    "27 Dresses",
    "The Talented Mr",
    "Ripley's Game",
    "Equilibrium",
    "One Hour Photo",
    "No Country for Old Men",
    "Labour Pains",
    "The Switch",
    "War Horse",
    "Good Will Hunting",
    "Frozen",
    "Bad Boys",
    "Frozen",
    "30 Minutes or Less",
    "The Cider House Rules",
    "Dallas Buyers Club",
    "Blue Is the Warmest Color",
    "When Harry Met Sally",
    "Sixteen Candles",
    "Family Guy Presents Stewie Griffin: The Untold Story",
    "Silver Linings Playbook",
]

genre_pool = [
    "Action",
    "Drama",
    "Horror",
    "Sci-fi",
    "Western",
    "Romance",
    "Comedy",
    "Musical",
    "Adventure",
    "Thriller"
]

countries_pool = [
    "USA",
    "Austria",
    "Russia",
    "France",
    "Switzerland",
    "Germany",
    "Bahrain",
    "Latvia",
    "South Korea",
    "French Southern Territories",
    "Peru",
    "Pitcairn",
    "Guinea",
    "Sweden",
    "Turkmenistan",
    "Haiti",
    "Cayman Islands",
    "Maldives",
    "Kyrgyzstan",
    "Barbados",
    "French Guiana",
    "Christmas Island",
    "Aland Islands",
    "Romania",
    "East Timor",
]

device_names_pool = [
    "IPhone",
    "IPad",
    "TV",
    "Toaster",
    "Xiaomi",
    "Samsung",
    "PC",
    "Laptop",
    "Smartphone",
    "Smart Watch"
]