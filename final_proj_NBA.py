from bs4 import BeautifulSoup
import sqlite3
import plotly
import requests
import json
from secrets import plotly_api
from secrets import plotly_username
import plotly.plotly as py
import plotly.graph_objs as go
import pandas as pd

plot = plotly_api
plotly__username = plotly_username
plotly.tools.set_credentials_file(username=plotly__username, api_key=plot)

CACHE_FNAME = 'cache.json'
try:
    cache_file = open(CACHE_FNAME, 'r')
    cache_contents = cache_file.read()
    CACHE_DICTION = json.loads(cache_contents)
    cache_file.close()

except:
    CACHE_DICTION = {}


def get_unique_key(url):
  return url


def make_request_using_cache(url):
    unique_ident = get_unique_key(url)

    if unique_ident in CACHE_DICTION:
        return CACHE_DICTION[unique_ident]


    else:
        resp = requests.get(url)

    CACHE_DICTION[unique_ident] = resp.text
    dumped_json_cache = json.dumps(CACHE_DICTION)
    fw = open(CACHE_FNAME,"w")
    fw.write(dumped_json_cache)
    fw.close()
    return CACHE_DICTION[unique_ident]

nba_team_list = {'Warriors': 1, 'Rockets': 2, 'Blazers': 3, 'Thunder': 4, 'Jazz': 5, 'Pelicans': 6, 'Timberwolves': 7, 'Spurs': 8, 'Suns': 9, 'Kings': 10, 'Lakers': 11, 'Nuggets': 12, 'Mavericks': 13, 'Grizzlies': 14,'Raptors': 15, 'Celtics': 16, '76ers': 17, 'Cavaliers': 18, 'Pacers': 19, 'Heat': 20, 'Bucks': 21, 'Wizards': 22, 'Pistons': 23, 'Hornets': 24, 'Knicks': 25, 'Bulls': 26, 'Magic': 27, 'Nets': 28, 'Hawks': 29, 'Clippers': 30}

def init_db():

    conn = sqlite3.connect('nba.db')
    cur = conn.cursor()

    statement = '''
        DROP TABLE IF EXISTS 'Games';
    '''
    cur.execute(statement)
    statement = '''
        DROP TABLE IF EXISTS 'Attendance';
    '''
    cur.execute(statement)
    conn.commit()
    statement = '''
    CREATE TABLE 'Games' (
            'Game Id' INTEGER PRIMARY KEY AUTOINCREMENT,
            'Home_Team' TEXT NOT NULL,
            'Home_Score' INTEGER NOT NULL,
            'Home_Team_ID' INTEGER NOT NULL,
            'Away_Team' TEXT NOT NULL,
            'Away_Score' INTEGER NOT NULL,
            'Away_Team_ID' INTEGER NOT NULL,
            'Point Differential' INTEGER NOT NULL
    );
    '''
    cur.execute(statement)
    conn.commit()

    statement = '''
        CREATE TABLE 'Attendance' (
                'Id' INTEGER PRIMARY KEY AUTOINCREMENT,
                'Home_Team' TEXT NOT NULL,
                'Home_Team_ID' INTEGER NOT NULL,
                'Point_Differential' INTEGER NOT NULL,
                'Result' TEXT NOT NULL,
                'Team_Attendance' INTEGER NOT NULL,
                'Weekday' TEXT NOT NULL,
                'Clock' INTEGER NOT NULL

        );
    '''
    cur.execute(statement)
    conn.commit()

def get_nba_scores(month):

    conn = sqlite3.connect('nba.db')
    cur = conn.cursor()

    baseurl = 'https://www.basketball-reference.com/leagues/NBA_2018_games-' + month + '.html'
    page_text = make_request_using_cache(baseurl)
    page_soup = BeautifulSoup(page_text, 'html.parser')


    nba_games = page_soup.find_all('tr')
    for row in nba_games[1:]:
        raw_day = row.find('th')
        tds = row.find_all('td')
        raw_away_team = tds[1].text.split(' ')
        raw_home_team = tds[3].text.split(' ')

        day = raw_day.text[:3]
        time = tds[0].text
        away_score = tds[2].text
        home_score = tds[4].text
        attendance = tds[7].text.replace(",","")

        if len(raw_away_team) == 3:
            away_team = raw_away_team[2]
        else:
            away_team = raw_away_team[1]

        if len(raw_home_team) == 3:
            home_team = raw_home_team[2]
        else:
            home_team = raw_home_team[1]
        away_team_id = nba_team_list[str(away_team)]
        home_team_id = nba_team_list[str(home_team)]

        point_diff = int(home_score) - int(away_score)
        if '-' in str(point_diff):
            result = 'L'
        else:
            result = 'W'

        insertion = (None, home_team, home_score, home_team_id, away_team, away_score, away_team_id, point_diff)
        statement = 'INSERT INTO "Games" '
        statement += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement, insertion)

        insertion1 = (None, home_team, home_team_id, point_diff, result, attendance, day, time)
        statement1 = 'INSERT INTO "Attendance" '
        statement1 += 'VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        cur.execute(statement1, insertion1)

    conn.commit()
    conn.close()

#AVG ATTENDANCE BY TEAM
def process_rankings(command):

    conn = sqlite3.connect('nba.db')
    cur = conn.cursor()

    info = []
    command_split = command.split(' ')
    option = command_split[1]
    if len(command_split) != 2:
        return 'invalid command'

    if option == 'rankings':
        statement = 'SELECT Home_Team, AVG(Team_Attendance) FROM Attendance '
        statement += 'GROUP BY Home_Team_ID '
        statement += 'ORDER BY AVG(Team_Attendance) DESC'

        cur.execute(statement)
        conn.commit()

        team = []
        amt = []
        layout = go.Layout(title = 'NBA Attendance Rankings',
        xaxis = dict(title = 'NBA Team'),
        yaxis = dict(title = 'Average Attendance')
        )
        for data in cur:
            team.append(data[0])
            amt.append(data[1])
            graph = [go.Bar(
            x=team,
            y=amt
            )]

        fig = go.Figure(data=graph, layout=layout)
        py.plot(fig, filename='NBA Attendance Rankings')


    else:
        return 'invalid command'

#Top 10 Time slots with highest average attendance
def process_times(command):

    conn = sqlite3.connect('nba.db')
    cur = conn.cursor()

    info = []
    command_split = command.split(' ')
    option = command_split[1]

    if option == 'times':
        statement = 'SELECT Weekday, Clock, AVG(Team_Attendance) FROM Attendance '
        statement += 'GROUP BY Clock '
        statement += 'ORDER BY AVG(Team_Attendance) '
        statement += 'DESC LIMIT 10'

    else:
        return 'invalid command'

    cur.execute(statement)
    conn.commit()

    date = []
    amt = []
    layout = go.Layout(title = 'Time Slots With Highest Average Attendance',
    xaxis = dict(title = 'Time Slot'),
    yaxis = dict(title = 'Average Attendance')
    )
    for data in cur:
        day = data[0]
        time = data[1]
        fulltime = time, day
        date.append(str(fulltime))
        amt.append(data[2])
        graph = [go.Bar(
        x=date,
        y=amt
        )]

    fig = go.Figure(data=graph, layout=layout)
    py.plot(fig, filename='Time Slots With Highest Average Attendance')

#Attendance based on Game Result
def process_attendance(command):

    conn = sqlite3.connect('nba.db')
    cur = conn.cursor()

    info = []
    command_split = command.split(' ')
    option = command_split[1]
    if len(command_split) != 2:
        return 'invalid command'

    else:
        statement = 'SELECT  Point_Differential, Team_Attendance FROM Attendance '
    cur.execute(statement)
    conn.commit()


    score = []
    amt = []
    layout = go.Layout(title = 'Point Differential vs. Attendance',
    xaxis = dict(title = 'Average Attendance'),
    yaxis = dict(title = 'Point Differential')
    )
    for data in cur:
        score.append(data[0])
        amt.append(data[1])
    trace1 = go.Scatter(
    x = amt,
    y = score,
    mode='markers',
    marker=dict(
        size='16',
        color = score,
        colorscale='Viridis',
        showscale=True
        ))

    data = [trace1]

    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename='Point Differential vs. Attendance')


#Graph projecting point totals at home vs road
def process_scores(command):

    conn = sqlite3.connect('nba.db')
    cur = conn.cursor()

    info = []
    command_split = command.split()
    if len(command_split) != 2:
        return 'invalid command'
    else:
        if command_split[1] in nba_team_list.keys():
            nickname = command_split[1]
            statement = 'SELECT Home_Score, Home_Team FROM Games '
            statement += 'WHERE Home_Team = "{}"'.format(nickname)

            statement1 = 'SELECT Away_Score, Away_Team FROM Games '
            statement1 += 'WHERE Away_Team = "{}"'.format(nickname)

        else:
            return 'invalid command'

    cur.execute(statement)
    conn.commit()

    home_scores = []
    away_scores = []
    count = 0
    for data in cur:
        home_scores.append(data[0])
        count += 1

    cur.execute(statement1)
    conn.commit()
    for data in cur:
        away_scores.append(data[0])
        count += 1

    home = go.Scatter(
    x = count,
    y = home_scores,
    mode = 'lines+markers',
    name = 'Home Games'
    )

    away = go.Scatter(
    x = count,
    y = away_scores,
    mode = 'lines+markers',
    name = 'Away Games'
    )
    data = [home, away]
    layout = go.Layout(title = '{} Road vs Home Scoring Totals'.format(nickname),
    xaxis = dict(title = 'Game Number'),
    yaxis = dict(title = 'Points Scored')
    )

    fig = go.Figure(data=data, layout=layout)
    py.plot(fig, filename='Road vs. Home Scoring totals')


def process_command(command):

    command_split = command.split()
    word = command_split[0]
    option = command_split[1]

    if 'attendance' not in command_split and 'scores' not in command_split:
        results = 'invalid command'

    elif word == 'attendance':

        if option == 'rankings':
            results = process_rankings(command)
        elif option == 'times':
            results = process_times(command)
        else:
            results = 'invalid command'

    else:

        if option == 'attendance':
            results = process_attendance(command)
            
        elif option in nba_team_list.keys():
            results = process_scores(command)
        else:
            results = "invalid command"

    return results

def load_help_text():
    with open('help.txt') as f:
        return f.read()

def interactive_prompt():
    help_text = load_help_text()
    response = ''

    while response != 'exit':
        response = input('Enter a command, or "help" for more options: ')
        print()

        if len(response.strip()) < 1:
            continue

        if response == 'help':
            print(help_text)
            continue

        if response == 'exit':
            print("Bye!")
            continue
        else:
            data = process_command(response)
            if data == "invalid command":
                print("Command not recognized: {}".format(response))


if __name__=="__main__":
    init_db()
    get_nba_scores('october')
    get_nba_scores('november')
    get_nba_scores('december')
    get_nba_scores('january')
    get_nba_scores('february')
    get_nba_scores('march')
    interactive_prompt()
