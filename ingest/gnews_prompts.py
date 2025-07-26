
def get_google_news_prompt(start_time, end_time, area, tags):
    return f"""
    You are an expert in news analysis. Your task is to analyze news articles and extract relevant information.
    Find news between {start_time} and {end_time} relating to areas = {area} in India, and extract the following, relating to tags={tags}:
    1. Summarise the news.
    2. Capture the last updated time or published time of the news with date.
    3. If the new has area and sublocation, capture it. If it has latitude and longitude capture that too.
    3. Capture the source of the news.
    
    !!NOTE: If there is no news strictly between the timestamps mentioned, just return "NO_NEWS". Dont add news published
    outside the time range given above.
    """

def get_structured_news_prompt(news):
    return f"""
    Given the following news in form of pointers, structure the text in required format, extracting key informations.
    News pointers: \n
    {news}
    """