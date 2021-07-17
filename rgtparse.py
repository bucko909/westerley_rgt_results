import lxml.etree, lxml.html
import os.path
import requests
import csv

def parse_time(time_str):
    if time_str.startswith('+ '):
        time_str = time_str[2:]
    if ':' in time_str:
        mins, secs = time_str.split(':')
        return int(mins) * 60 + float(secs)
    else:
        return float(time_str)

def get_events():
    csvfile = open(f'data/events.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        yield row[0]

def get_teams():
    teams = {}
    csvfile = open(f'data/teams.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        teams[row[0]] = row[1]
    return teams

def update_events():
    users = {}
    teams = get_teams()
    team_points = {}
    if not os.path.exists('out'):
        os.mkdir('out')
    if not os.path.exists('cache'):
        os.mkdir('cache')
    for race_no, race_id in enumerate(get_events(), start=1):
        if os.path.exists(f'cache/{race_id}.html'):
            html = open(f'cache/{race_id}.html','r').read()
        else:
            response = requests.get(f'https://rgtdb.com/events/{race_id}')
            if not response.ok:
                import pdb; pdb.set_trace()
            html = response.text
            open(f'cache/{race_id}.html', 'w').write(html)
        csvfile = open(f'out/{race_no:02n}_{race_id}.csv', 'w')
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['pos', 'userurl', 'name', 'rgt_teamname', 'vr_teamname', 'team_qualifier', 'time_secs', 'delta_secs', 'wkg'])
        team_results_dict = {}
        for row in parse_event(html, teams):
            csvwriter.writerow(row)
            ingest_row(row, users, team_points)
            vr_teamname, team_qualifier = row[4:6]
            if team_qualifier:
                team_results_dict.setdefault(vr_teamname, []).append(row[0])
        write_users(users, f'out/{race_no:02n}_{race_id}_users_cumulative.csv')
        write_teams(team_points, f'out/{race_no:02n}_{race_id}_teams_cumulative.csv')

    write_users(users)
    write_teams(team_points)

def write_users(users, fname='out/user_results.csv'):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'url', 'name', 'points', 'team', 'team points', 'best', 'race count', 'win count', '2nd count', '3rd count'])
    users = list(users.items())
    users.sort(key=lambda u: u[1]['points'], reverse=True)
    for pos, u in enumerate(users, start=1):
        csvwriter.writerow([pos, 'https://rgtdb.com' + u[0], u[1]['name'], u[1]['points'], u[1]['team'], u[1]['team_points'], u[1]['best'], u[1]['races'], u[1]['golds'], u[1]['silvers'], u[1]['bronzes']])

def write_teams(teams, fname='out/team_results.csv'):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'name', 'points'])
    teams = list(teams.items())
    teams.sort(key=lambda t: t[1], reverse=True)
    for pos, t in enumerate(teams, start=1):
        csvwriter.writerow([pos, t[0], t[1]])

def parse_event(html, teams):
    data = lxml.etree.HTML(html)
    vr_team_runners = dict()
    for result in data.xpath('/html/body/div/main/div/div/div[@id="results"]/table/tbody/tr'):
        postd, _trophytd, userdatatd, timetd, wkgtd, _rankchangetd = result.xpath('td')
        dnfspan = postd.xpath('span')
        if dnfspan:
            if postd.xpath('span/text()') != ['DNF']:
                raise Exception(lxml.html.tostring(postd))
            pos = None
        else:
            post, = postd.xpath('text()')
            pos = int(post.strip())
        userlinks = userdatatd.xpath('span/a')
        if userlinks:
            userlink, = userlinks
            userurl = userlink.attrib['href']
            username, = userlink.xpath('text()')
        else:
            userurl = None
            usernamebits = [x.strip() for x in userdatatd.xpath('span/text()')]
            if usernamebits[0] != '':
                raise Exception(lxml.html.tostring(userdatatd))
            username, = usernamebits[1:]
        teamnamespans = userdatatd.xpath('a/span')
        if teamnamespans:
            teamnamespan, = teamnamespans
            rgt_teamname = teamnamespan.text.strip()
        else:
            rgt_teamname = None
        if pos is not None:
            timet, _blank = timetd.xpath('text()')
            time = parse_time(timet.strip())
            deltats = timetd.xpath('em/text()')
            if deltats:
                deltat, = deltats
                delta = parse_time(deltat.strip())
            else:
                delta = 0.0
            wkgt, = wkgtd.xpath('text()')
            wkg = float(wkgt.strip())
        else:
            time = None
            delta = None
            wkg = None
        vr_teamname = teams.get(userurl)
        if vr_teamname:
            vr_team_finishers = vr_team_runners.setdefault(vr_teamname, [])
            vr_team_finishers.append(userurl)
            if len(vr_team_finishers) <= 2:
                team_qualifier = True
            else:
                team_qualifier = False
        else:
            team_qualifier = None
        yield (pos, userurl, username, rgt_teamname, vr_teamname, team_qualifier, time, delta, wkg)

def ingest_row(row, users, team_points):
    pos, userurl, username, rgt_teamname, vr_teamname, team_qualifier, time, delta, wkg = row
    if pos:
        if userurl is None:
            raise Exception(lxml.html.tostring(result))
        users.setdefault(userurl, {'name': username, 'points': 0, 'team_points': 0, 'races': 0, 'golds': 0, 'silvers': 0, 'bronzes': 0, 'team': vr_teamname})
        user = users[userurl]
        assert vr_teamname == user['team'], (vr_teamname, user)
        user.setdefault('best', pos)
        user['best'] = min(pos, user['best'])
        if pos == 1:
            user['golds'] += 1
        elif pos == 2:
            user['silvers'] += 1
        elif pos == 3:
            user['bronzes'] += 1
        race_points = max(1, 101 - pos)
        user['points'] += race_points
        user['races'] += 1
        if team_qualifier:
            team_points.setdefault(vr_teamname, 0)
            team_points[vr_teamname] += race_points
            user['team_points'] += race_points

if __name__ == '__main__':
    update_events()
