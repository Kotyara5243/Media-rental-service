class Family:
    def __init__(
        self,
        family_id: int | None,
        family_type: str,
        creation_date: str
    ):
        self.family_id = family_id
        self.family_type = family_type
        self.creation_date = creation_date

    @staticmethod
    def from_row(row: dict) -> "Family":
        return Family(
            family_id=row["family_id"],
            family_type=row["family_type"],
            creation_date=row["creation_date"]
        )


class User:
    def __init__(
        self,
        user_id: int | None,
        user_name: str,
        email: str,
        birthday: str,
        location: str,
        bio: str,
        family_id: int | None
    ):
        self.user_id = user_id
        self.user_name = user_name
        self.email = email
        self.birthday = birthday
        self.location = location
        self.bio = bio
        self.family_id = family_id

    @staticmethod
    def from_row(row: dict) -> "User":
        return User(
            user_id=row["user_id"],
            user_name=row["user_name"],
            email=row["email"],
            birthday=row["birthday"],
            location=row["location"],
            bio=row["bio"],
            family_id=row["family_id"]
        )


class Friendship:
    def __init__(self, user_id: int, friend_id: int):
        self.user_id = user_id
        self.friend_id = friend_id

    @staticmethod
    def from_row(row: dict) -> "Friendship":
        return Friendship(
            user_id=row["user_id"],
            friend_id=row["friend_id"]
        )


class Media:
    def __init__(
        self,
        media_id: int | None,
        media_name: str,
        genre: str,
        prod_year: int,
        descr: str,
        location: str,
        cost_per_day: int
    ):
        self.media_id = media_id
        self.media_name = media_name
        self.genre = genre
        self.prod_year = prod_year
        self.descr = descr
        self.location = location
        self.cost_per_day = cost_per_day

    @staticmethod
    def from_row(row: dict) -> "Media":
        return Media(
            media_id=row["media_id"],
            media_name=row["media_name"],
            genre=row["genre"],
            prod_year=row["prod_year"],
            descr=row["descr"],
            location=row["location"],
            cost_per_day=row["cost_per_day"]
        )


class Series:
    def __init__(
        self,
        series_id: int | None,
        number_of_episodes: int,
        is_ongoing: bool,
        media_id: int
    ):
        self.series_id = series_id
        self.number_of_episodes = number_of_episodes
        self.is_ongoing = is_ongoing
        self.media_id = media_id

    @staticmethod
    def from_row(row: dict) -> "Series":
        return Series(
            series_id=row["series_id"],
            number_of_episodes=row["number_of_episodes"],
            is_ongoing=row["is_ongoing"],
            media_id=row["media_id"]
        )


class Film:
    def __init__(
        self,
        film_id: int | None,
        duration: int,
        number_of_parts: int,
        media_id: int
    ):
        self.film_id = film_id
        self.duration = duration
        self.number_of_parts = number_of_parts
        self.media_id = media_id

    @staticmethod
    def from_row(row: dict) -> "Film":
        return Film(
            film_id=row["film_id"],
            duration=row["duration"],
            number_of_parts=row["number_of_parts"],
            media_id=row["media_id"]
        )


class Session:
    def __init__(
        self,
        session_id: int | None,
        user_id: int,
        media_id: int,
        date_of_rent: str,
        cost: int,
        duration: int
    ):
        self.session_id = session_id
        self.user_id = user_id
        self.media_id = media_id
        self.date_of_rent = date_of_rent
        self.cost = cost
        self.duration = duration

    @staticmethod
    def from_row(row: dict) -> "Session":
        return Session(
            session_id=row["session_id"],
            user_id=row["user_id"],
            media_id=row["media_id"],
            date_of_rent=row["date_of_rent"],
            cost=row["cost"],
            duration=row["duration"]
        )


class WatchHistory:
    def __init__(
        self,
        watch_history_id: int | None,
        user_id: int,
        media_id: int,
        date_of_watch: str,
        family_watch: bool
    ):
        self.watch_history_id = watch_history_id
        self.user_id = user_id
        self.media_id = media_id
        self.date_of_watch = date_of_watch
        self.family_watch = family_watch

    @staticmethod
    def from_row(row: dict) -> "WatchHistory":
        return WatchHistory(
            watch_history_id=row["watch_history_id"],
            user_id=row["user_id"],
            media_id=row["media_id"],
            date_of_watch=row["date_of_watch"],
            family_watch=row["family_watch"]
        )


class Device:
    def __init__(
        self,
        device_id: int | None,
        device_name: str,
        registration_date: str,
        user_id: int
    ):
        self.device_id = device_id
        self.device_name = device_name
        self.registration_date = registration_date
        self.user_id = user_id

    @staticmethod
    def from_row(row: dict) -> "Device":
        return Device(
            device_id=row["device_id"],
            device_name=row["device_name"],
            registration_date=row["registration_date"],
            user_id=row["user_id"]
        )
