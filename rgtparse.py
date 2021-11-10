import lxml.etree, lxml.html
import os.path
import shutil
import requests
import csv
import datetime

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
        if len(row) > 3:
            extras = []
            after = None
            for i in range(1, (len(row)-1) // 2):
                start, end = row[i*2+1:i*2+3]
                if end:
                    if not start:
                        start = 1
                    else:
                        start = int(start)
                    end = int(end)
                    extras.extend(range(start, end+1))
                elif start:
                    assert after is None
                    after = int(start)
                else:
                    assert after is None and len(extras) == 0
        else:
            extras = None
            after = None
        teams[row[0]] = {'name': row[2], 'after': after, 'extras': extras}
    return teams

def get_westerley():
    members = {}
    csvfile = open(f'data/westerley.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    for url, name, after in csvreader:
        members[url] = int(after) if after else 0
    return members

def get_countries():
    countries = {}
    csvfile = open(f'data/countries.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    for url, name, dave_name, country in csvreader:
        if country != '':
            countries[url.replace('https://rgtdb.com', '')] = country
    return countries

def update_events():
    users = {}
    teams = get_teams()
    westerley = get_westerley()
    countries = get_countries()
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
        csvwriter.writerow(['pos', 'userurl', 'name', 'rgt_teamname', 'vr_teamname', 'team_qualifier', 'westerley_pos', 'time_secs', 'delta_secs', 'wkg', 'time_human', 'delta_human'])
        for row in parse_event(html, teams, westerley, race_no):
            csvwriter.writerow(row + tuple(map(lambda x: (datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=x)).strftime('%M:%S.%f')[:-3] if x is not None else None, row[7:9])))
            ingest_row(row, users, team_points)
            vr_teamname, team_qualifier = row[4:6]
        try:
            check_csv = csv.reader(open(f'data/team_after_{race_no}.csv', 'r'))
        except FileNotFoundError:
            pass
        # Ugly team verification code.
        #print(race_no)
        #for team_name, pointss in check_csv:
        #    points = int(pointss)
        #    aliases = {'BRT': 'TEAM BTR', 'TEAM BRT': 'TEAM BTR'}
        #    team_name = aliases.get(team_name, team_name)
        #    if points != team_points.get(team_name):
        #        print(team_name, points, team_points.get(team_name), team_points.get(team_name,0) - points)
        write_users(users, countries, f'out/{race_no:02n}_{race_id}_users_cumulative.csv')
        write_westerley(users, f'out/{race_no:02n}_{race_id}_westerley_cumulative.csv')
        write_teams(team_points, f'out/{race_no:02n}_{race_id}_teams_cumulative.csv')
        try:
            os.unlink('out/user_results.csv')
        except:
            pass
        try:
            os.unlink('out/westerley_results.csv')
        except:
            pass
        try:
            os.unlink('out/team_results.csv')
        except:
            pass
        shutil.copyfile(f'out/{race_no:02n}_{race_id}_users_cumulative.csv', 'out/user_results.csv')
        shutil.copyfile(f'out/{race_no:02n}_{race_id}_westerley_cumulative.csv', 'out/westerley_results.csv')
        shutil.copyfile(f'out/{race_no:02n}_{race_id}_teams_cumulative.csv', 'out/team_results.csv')
    #update_teams(users)

def update_teams(users):
    csvfile = open(f'data/teams.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    newrows = []
    for row in csvreader:
        if row[0][0] == '/':
            newrow = [row[0], users.get(row[0], {}).get('name', '')] + row[1:]
        else:
            newrow = ['', row[0]] + row[1:]
        newrows.append(newrow)
    csvfile = open(f'data/teams_new.csv', 'w')
    csvwriter = csv.writer(csvfile)
    for row in newrows:
        csvwriter.writerow(row)

def write_users(users, countries, fname='out/user_results.csv'):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'last_pos', 'change', 'url', 'name', 'country', 'points', 'best6', 'team', 'team points', 'westerley points', 'best', 'race count', 'win count', '2nd count', '3rd count'])
    users = list(users.items())
    users.sort(key=lambda u: sum(u[1]['results']), reverse=True)
    for pos, u in enumerate(users, start=1):
        if 'last_pos' in u[1]:
            last_pos = u[1]['last_pos']
            if u[1]['last_pos'] > pos:
                change = '▲'
            elif u[1]['last_pos'] < pos:
                change = '▼'
            else:
                change = '-'
        else:
            last_pos = ''
            change = '*'
        csvwriter.writerow([pos, last_pos, change, 'https://rgtdb.com' + u[0], u[1]['name'], countries.get(u[0]), sum(u[1]['results']), sum(sorted(u[1]['results'], reverse=True)[:6]), u[1]['team'], u[1]['team_points'], u[1]['westerley_points'], 101 - max(u[1]['results']), len(u[1]['results']), sum(1 for x in u[1]['results'] if x == 100), sum(1 for x in u[1]['results'] if x == 99), sum(1 for x in u[1]['results'] if x == 98)])
        u[1]['last_pos'] = pos

def write_westerley(users, fname='out/westerley_results.csv'):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'url', 'name', 'westerley points', 'race count'])
    users = list(users.items())
    users.sort(key=lambda u: u[1]['westerley_points'], reverse=True)
    for pos, u in enumerate(users, start=1):
        if u[1]['westerley_points'] == 0:
            continue
        csvwriter.writerow([pos, 'https://rgtdb.com' + u[0], u[1]['name'], u[1]['westerley_points'], len(u[1]['results'])])

def write_teams(teams, fname='out/team_results.csv'):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'name', 'points'])
    teams = list(teams.items())
    teams.sort(key=lambda t: t[1], reverse=True)
    for pos, t in enumerate(teams, start=1):
        csvwriter.writerow([pos, t[0], t[1]])

def parse_event(html, teams, westerley, race_no):
    data = lxml.etree.HTML(html)
    vr_team_runners = dict()
    westerley_seen = set()
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
        vr_team = teams.get(userurl)
        if vr_team is not None and is_in_range(vr_team, race_no):
            vr_teamname = vr_team['name']
            vr_team_finishers = vr_team_runners.setdefault(vr_teamname, [])
            vr_team_finishers.append(userurl)
            if len(vr_team_finishers) <= 2:
                team_qualifier = True
            else:
                team_qualifier = False
        else:
            vr_teamname = None
            team_qualifier = None
        if westerley.get(userurl, float('inf')) <= race_no:
            westerley_seen.add(userurl)
            westerley_pos = len(westerley_seen)
        else:
            westerley_pos = None
        yield (pos, userurl, username, rgt_teamname, vr_teamname, team_qualifier, westerley_pos, time, delta, wkg)

def is_in_range(vr_team, race_no):
    if vr_team['extras'] is None and vr_team['after'] is None:
        return True
    if vr_team['after'] is not None:
        return race_no >= vr_team['after']
    return race_no in vr_team['extras']

def ingest_row(row, users, team_points):
    pos, userurl, username, rgt_teamname, vr_teamname, team_qualifier, westerley_pos, time, delta, wkg = row
    if pos:
        if userurl is None:
            raise Exception(lxml.html.tostring(result))
        users.setdefault(userurl, {'name': username, 'team_points': 0, 'westerley_points': 0, 'results': [], 'team': vr_teamname})
        user = users[userurl]
        if user['team'] is None or vr_teamname is None:
            # allow flips between None
            user['team'] = vr_teamname
        assert vr_teamname == user['team'], (vr_teamname, user)
        race_points = max(1, 101 - pos)
        user['results'].append(race_points)
        if team_qualifier:
            team_points.setdefault(vr_teamname, 0)
            team_points[vr_teamname] += race_points
            user['team_points'] += race_points
        if westerley_pos is not None:
            user['westerley_points'] += max(1, 21 - westerley_pos)

if __name__ == '__main__':
    update_events()
