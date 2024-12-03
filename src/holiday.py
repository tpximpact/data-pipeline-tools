"""Get UK bank holidays."""

from datetime import datetime, timedelta

import holidays
import pandas as pd

SATURDAY = 5
SUNDAY = 6
CURRENT_YEAR = datetime.now().year


def get_uk_holidays(year: int = CURRENT_YEAR) -> pd.DataFrame:
    """Get holidays for England in a given year.

    Args:
    ----
        year: The year to get holidays for.

    Returns:
    -------
        pd.DataFrame: A DataFrame of holidays for England in the given year.

    """

    def get_spent_dates(date: datetime.date) -> datetime.date:
        if date.weekday() == SATURDAY:
            return date + timedelta(days=2)
        if date.weekday() == SUNDAY:
            return date + timedelta(days=1)
        return date

    holiday_list = [{"date": date, "name": name} for date, name in holidays.UK(years=year).items()]
    holidays_df = pd.DataFrame(holiday_list).sort_values("date").reset_index(drop=True)
    holidays_df["spent_date"] = holidays_df["date"].apply(lambda date: get_spent_dates(date))
    return holidays_df[~holidays_df["name"].str.contains(r"\[Scotland\]") & ~holidays_df["name"].str.contains(r"\[Northern Ireland\]")]
