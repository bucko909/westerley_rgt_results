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

def get_teams(race_no):
    teams = {}
    csvfile = open(f'data/teams.csv', 'r', newline='')
    csvreader = csv.reader(csvfile)
    for row in csvreader:
        if len(row) == 0:
            continue
        if row[0].startswith('#'):
            continue
        if len(row) > 3:
            allowed = False
            for i in range(1, (len(row)-1) // 2):
                start, end = row[i*2+1:i*2+3]
                if end and int(end) < race_no:
                    continue
                elif start and int(start) > race_no:
                    continue
                allowed = True
        else:
            allowed = True
        if allowed:
            teams[row[0]] = row[2]
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
    westerley = get_westerley()
    countries = get_countries()
    team_points = {}
    if not os.path.exists('out'):
        os.mkdir('out')
    if not os.path.exists('cache'):
        os.mkdir('cache')
    for race_no, race_id in enumerate(get_events(), start=1):
        teams = get_teams(race_no)
        if os.path.exists(f'cache/{race_id}.html'):
            html = open(f'cache/{race_id}.html','r').read()
        else:
            response = requests.get(f'https://rgtdb.com/events/{race_id}')
            if not response.ok:
                import pdb; pdb.set_trace()
            html = response.text
            open(f'cache/{race_id}.html', 'w').write(html)
        results = list(parse_event(html, westerley, race_no))
        team_runners = process_teams(results, teams)
        write_race_results(results, f'out/{race_no:02n}_{race_id}.csv')
        for row in results:
            ingest_row(row, users, team_points)
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
        write_users(users, countries, f'out/{race_no:02n}_{race_id}_users_cumulative_best6.csv', best6=True)
        write_westerley(users, f'out/{race_no:02n}_{race_id}_westerley_cumulative.csv')
        write_race_teams(team_runners, users, f'out/{race_no:02n}_{race_id}_teams.csv')
        write_teams(team_points, f'out/{race_no:02n}_{race_id}_teams_cumulative.csv')
        check_teams(team_points, f'data/official_team_results_after_{race_no}.csv')
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
        shutil.copyfile(f'out/{race_no:02n}_{race_id}_users_cumulative_best6.csv', 'out/user_results_best6.csv')
        shutil.copyfile(f'out/{race_no:02n}_{race_id}_westerley_cumulative.csv', 'out/westerley_results.csv')
        shutil.copyfile(f'out/{race_no:02n}_{race_id}_teams_cumulative.csv', 'out/team_results.csv')
    #update_teams(users)

TEAM_ALIASES = {
    'MOON RIDERS 2': 'SATELITE RIDERS',
    'TWICKENHAM CC 1': 'TWICKENHAM CC',
    'TWICKENHAM CC 2': 'TWICKENHAM CC LADIES',
}
def check_teams(team_points, official_fname):
    try:
        csvfile = open(official_fname, 'r', newline='')
    except:
        print(f"Couldn't open team results file {official_fname}")
        return
    csvreader = csv.reader(csvfile)
    seen = set()
    for team_name, official_points_str in csvreader:
        official_points = int(official_points_str)
        team_name = TEAM_ALIASES.get(team_name, team_name)
        seen.add(team_name)
        computed_points = team_points.get(team_name)
        team_points[team_name] = official_points
        if computed_points is None:
            print(f"NOCOMP: Team {team_name} in {official_fname} but not in computed results")
            continue
        delta = computed_points - official_points
        if delta != 0:
            print(f"BADCOMP: Team {team_name} has {official_points} in {official_fname} but computed {computed_points} (delta={delta}).")
    for team_name in team_points:
        if team_name not in seen:
            print(f"NOOFF: Team {team_name} has computed results but is not in {official_fname}.")


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

def process_teams(race_results, teams):
    team_results = dict()
    for row in race_results:
        row['vr_teamname'] = teams.get(row['userurl'])
        if row['vr_teamname'] is not None:
            this_team_results = team_results.setdefault(row['vr_teamname'], {'name': row['vr_teamname'], 'runners': [], 'points': 0})
            this_team_results['runners'].append(row['userurl'])
            if len(this_team_results['runners']) <= 2 and row['pos'] is not None:
                row['team_qualifier'] = True
                this_team_results['points'] += 101 - row['pos']
            else:
                row['team_qualifier'] = False
        else:
            row['team_qualifier'] = None
    return team_results

def write_race_teams(team_results, users, fname):
    csvwriter = csv.writer(open(fname, 'w'))
    csvwriter.writerow(['name', 'points', 'first', 'second', 'third', 'fourth', 'extras(!)'])
    team_results = list(team_results.values())
    team_results.sort(key=lambda x: -x['points'])
    for team in team_results:
        row = [team['name'], team['points']]
        runners = [users[url]['name'] for url in team['runners']]
        while len(runners) < 4:
            runners.append(None)
        row.extend(runners[:4])
        row.append(','.join(runners[4:]))
        csvwriter.writerow(row)

def format_human_delta(seconds):
    return (datetime.datetime(2000, 1, 1) + datetime.timedelta(seconds=seconds)).strftime('%M:%S.%f')[:-3]

def write_race_results(results, fname):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    keys = ['pos', 'userurl', 'name', 'rgt_teamname', 'vr_teamname', 'team_qualifier', 'westerley_pos', 'time_secs', 'delta_secs', 'wkg']
    csvwriter.writerow(keys + ['time_human', 'delta_human'])
    for row in results:
        if row['time_secs'] is not None:
            time_human = format_human_delta(row['time_secs'])
            delta_human = format_human_delta(row['delta_secs'])
        else:
            time_human = delta_human = None
        outrow = [row[key] for key in keys] + [time_human, delta_human]
        csvwriter.writerow(outrow)

def write_users(users, countries, fname='out/user_results.csv', best6=False):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'last_pos', 'change', 'url', 'name', 'country', 'points', 'best6', 'team', 'team points', 'westerley points', 'best', 'race count', 'win count', '2nd count', '3rd count'])
    users = list(users.items())
    if best6:
        users.sort(key=lambda u: sum(sorted(u[1]['results'], reverse=True)[:6]), reverse=True)
        last_pos_field = 'last_pos_best6'
    else:
        users.sort(key=lambda u: (sum(u[1]['results']), u[1]['name']), reverse=True)
        last_pos_field = 'last_pos'
    old_total = old_pos = None
    for pos, u in enumerate(users, start=1):
        total = sum(u[1]['results'])
        if total == old_total:
            pos = old_pos
        old_total = total
        old_pos = pos
        if len(u[1]['results']) == 0:
            continue
        if last_pos_field in u[1]:
            last_pos = u[1][last_pos_field]
            if u[1][last_pos_field] > pos:
                change = '▲'
            elif u[1][last_pos_field] < pos:
                change = '▼'
            else:
                change = '-'
        else:
            last_pos = ''
            change = '*'
        csvwriter.writerow([pos, last_pos, change, 'https://rgtdb.com' + u[0], u[1]['name'], countries.get(u[0]), sum(u[1]['results']), sum(sorted(u[1]['results'], reverse=True)[:6]), u[1]['team'], u[1]['team_points'], u[1]['westerley_points'], 101 - max(u[1]['results']), len(u[1]['results']), sum(1 for x in u[1]['results'] if x == 100), sum(1 for x in u[1]['results'] if x == 99), sum(1 for x in u[1]['results'] if x == 98)])
        u[1][last_pos_field] = pos

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

def write_teams(team_points, fname='out/team_results.csv'):
    csvfile = open(fname, 'w')
    csvwriter = csv.writer(csvfile)
    csvwriter.writerow(['pos', 'name', 'points'])
    team_points = list(team_points.items())
    team_points.sort(key=lambda t: t[1], reverse=True)
    for pos, t in enumerate(team_points, start=1):
        csvwriter.writerow([pos, t[0], t[1]])

def parse_event(html, westerley, race_no):
    data = lxml.etree.HTML(html)
    westerley_seen = set()
    for result in data.xpath('/html/body/div/main/div/div/div[@id="results"]/table/tbody/tr'):
        row = {}
        postd, _trophytd, userdatatd, timetd, wkgtd, _rankchangetd = result.xpath('td')
        dnfspan = postd.xpath('span')
        if dnfspan:
            if postd.xpath('span/text()') != ['DNF']:
                raise Exception(lxml.html.tostring(postd))
            row['pos'] = None
        else:
            post, = postd.xpath('text()')
            row['pos'] = int(post.strip())
        userlinks = userdatatd.xpath('span/a')
        if userlinks:
            userlink, = userlinks
            row['userurl'] = userlink.attrib['href']
            row['name'], = userlink.xpath('text()')
        else:
            row['userurl'] = None
            usernamebits = [x.strip() for x in userdatatd.xpath('span/text()')]
            if usernamebits[0] != '':
                raise Exception(lxml.html.tostring(userdatatd))
            row['name'], = usernamebits[1:]
        if row['pos'] is not None and row['userurl'] is None:
            raise Exception(lxml.html.tostring(result))
        teamnamespans = userdatatd.xpath('a/span')
        if teamnamespans:
            teamnamespan, = teamnamespans
            row['rgt_teamname'] = teamnamespan.text.strip()
        else:
            row['rgt_teamname'] = None
        if row['pos'] is not None:
            timet, _blank = timetd.xpath('text()')
            row['time_secs'] = parse_time(timet.strip())
            deltats = timetd.xpath('em/text()')
            if deltats:
                deltat, = deltats
                row['delta_secs'] = parse_time(deltat.strip())
            else:
                row['delta_secs'] = 0.0
            wkgt, = wkgtd.xpath('text()')
            row['wkg'] = float(wkgt.strip())
        else:
            row['time_secs'] = None
            row['delta_secs'] = None
            row['wkg'] = None
        if westerley.get(row['userurl'], float('inf')) <= race_no:
            westerley_seen.add(row['userurl'])
            row['westerley_pos'] = len(westerley_seen)
        else:
            row['westerley_pos'] = None
        yield row

def ingest_row(row, users, team_points):
    users.setdefault(row['userurl'], {'name': row['name'], 'team_points': 0, 'westerley_points': 0, 'results': [], 'team': row['vr_teamname']})
    if row['pos']:
        user = users[row['userurl']]
        if user['team'] != row['vr_teamname'] and row['vr_teamname'] is not None:
            if user['team'] is not None:
                print(f"TEAMCHANGE: {row['userurl']} changes to team {row['vr_teamname']}")
            user['team'] = row['vr_teamname']
        race_points = max(1, 101 - row['pos'])
        user['results'].append(race_points)
        if row['team_qualifier']:
            team_points.setdefault(row['vr_teamname'], 0)
            team_points[row['vr_teamname']] += race_points
            user['team_points'] += race_points
        if row['westerley_pos'] is not None:
            user['westerley_points'] += max(1, 21 - row['westerley_pos'])

if __name__ == '__main__':
    update_events()
