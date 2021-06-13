import lxml.etree, lxml.html

def parse_time(time_str):
    if time_str.startswith('+ '):
        time_str = time_str[2:]
    if ':' in time_str:
        mins, secs = time_str.split(':')
        return int(mins) * 60 + float(secs)
    else:
        return float(time_str)


data = lxml.etree.HTML(open('67390','r').read())
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
        teamname = teamnamespan.text.strip()
    else:
        teamname = None
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
    print(pos, userurl, username, teamname, time, delta, wkg)
