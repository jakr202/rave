import dbfunc4 as d4
from pathlib import Path
import csv
import time
from datetime import datetime
import sys

start_time = time.time()
bulkpath = Path.home()/'PycharmProjects/TM470/handHistory-24264.txt'
manypath = Path.home()/'OneDrive/Documents/pokerHistory/Jakr1'

'''run one tupletourlist depending on if I make random changs what happens 
whether the bulk file or the many single files needs to be processed'''

# tupletourlist = d4.loadbulklist(bulkpath)
tupletourlist = d4.loadmanylist(manypath)
if not tupletourlist:
    sys.exit()

conn = d4.createConnection()
cur = conn.cursor()
handFile = open('hand.csv', 'w', newline='')
handWriter = csv.writer(handFile)
handplayerFile = open('handplayer.csv', 'w', newline='')
handplayerWriter = csv.writer(handplayerFile)
handactionFile = open('handaction.csv', 'w', newline='')
handactionWriter = csv.writer(handactionFile)
playerdict = {}

'''create tournament table entries for entire list of tournaments and create list of hands'''
print('Loading tournament and player table data to database...')
for t in range(len(tupletourlist)):
    d4.loadtournament(tupletourlist[t][0])
    t_id = d4.tourRegex.findall(tupletourlist[t][0])[0][3]
    handlist = d4.gethandlist(tupletourlist[t])
    mo_1 = d4.dateRegex.findall(handlist[-1][0])
    datetime_object = datetime.strptime(mo_1[0], '%Y/%m/%d %H:%M:%S')
    end_time = datetime_object.strftime('%Y-%m-%d %H:%M:%S')
    for line in tupletourlist[t]:
        if d4.jakr1Regex.search(line) is not None:
            mo = d4.jakr1Regex.findall(line)
            win = mo[0]
            result = '1st'
            cur.execute("UPDATE tournament SET result =%s, win =%s, end_time =%s WHERE id =%s",
                        (result, win, end_time, t_id))
            conn.commit()
        elif d4.jakr2Regex.search(line) is not None:
            mo = d4.jakr2Regex.findall(line)
            if mo[0][2] == '':
                win = 0
            else:
                win = mo[0][2]
            result = mo[0][0]
            cur.execute("UPDATE tournament SET result =%s, win =%s, end_time =%s WHERE id =%s",
                        (result, win, end_time, t_id))
            conn.commit()
    '''create player table entries and player dictionary for entire list of tournaments'''
    for i in range(2, 10):
        while d4.seatRegex.search(tupletourlist[t][i]) is not None:
            mos = d4.seatRegex.findall(tupletourlist[t][i])
            handle = mos[0][1]
            p_id = d4.getplayerid(mos[0][1])
            playerdict.update({handle: p_id})
            break
print('Tournament and player load completed in' + "--- %s seconds ---" % (time.time() - start_time))

'''create hand table entries for entire list of tournaments'''
print('Loading hand data to database...')
for t in range(len(tupletourlist)):
    handlist = d4.gethandlist(tupletourlist[t])
    for hand in handlist:
        h_id = d4.gethandid(hand)
        h_line, f_line, t_line, r_line, sd_line, sum_line = d4.getstagelines(hand)
        hole = d4.gethole(hand, h_line + 1)
        flop = None
        turn = None
        river = None
        ante = None
        sb = None
        bb = None
        antec = False
        for line in range(3, h_line):
            if d4.postRegex.search(hand[line]) is not None:
                mo = d4.postRegex.findall(hand[line])
                if mo[0][2] == 'ante' and antec is False:
                    ante = mo[0][3]
                    antec = True
                elif mo[0][2] == 'small blind':
                    sb = mo[0][3]
                elif mo[0][2] == 'big blind':
                    bb = mo[0][3]
        for line in range(h_line, sum_line):
            if d4.flopRegex.search(hand[line]) is not None:
                mo = d4.flopRegex.findall(hand[line])
                flop = mo[0]
            elif d4.turnRegex.search(hand[line]) is not None:
                mo = d4.turnRegex.findall(hand[line])
                turn = mo[0]
            elif d4.riverRegex.search(hand[line]) is not None:
                mo = d4.riverRegex.findall(hand[line])
                river = mo[0]
        handWriter.writerow([h_id, ante, sb, bb, hole, flop, turn, river])
handFile.close()
cur.execute(
    "LOAD DATA LOCAL INFILE 'C:/Users/jkram/PycharmProjects/TM470/hand.csv' INTO TABLE hand FIELDS "
    "TERMINATED BY ',' LINES TERMINATED BY '\r\n' (id, ante, sb, bb, hole, flop, turn, river)")
conn.commit()
print('Hand load completed at' + "--- %s seconds ---" % (time.time() - start_time))

'''create handplayer table entries for entire list of tournaments'''
print('Loading handplayer data to database...')
for t in range(len(tupletourlist)):
    handlist = d4.gethandlist(tupletourlist[t])
    for hand in handlist:
        h_line, f_line, t_line, r_line, sd_line, sum_line = d4.getstagelines(hand)
        t_id = d4.gettournamentid(hand)
        h_id = d4.gethandid(hand)
        b_seat = d4.getbuttonseat(hand)
        bbp = d4.getbbp(hand)
        sbp = d4.getsbp(hand)
        for i in range(2, h_line):
            if d4.seatRegex.search(hand[i]) is not None:
                mos = d4.seatRegex.findall(hand[i])
                p_id = playerdict.get(mos[0][1])
                seat = int(mos[0][0])
                start_stack = mos[0][2]
                if seat != b_seat and p_id != playerdict.get(bbp) and p_id != playerdict.get(sbp):
                    position = None
                elif seat == b_seat:
                    position = 'button'
                elif p_id == playerdict.get(bbp):
                    position = 'big blind'
                if p_id == playerdict.get(sbp):
                    position = 'small blind'
                handplayerWriter.writerow([t_id, p_id, h_id, seat, start_stack, position])
handplayerFile.close()
cur.execute(
    "LOAD DATA LOCAL INFILE 'C:/Users/jkram/PycharmProjects/TM470/handplayer.csv' INTO TABLE handplayer FIELDS "
    "TERMINATED BY ',' LINES TERMINATED BY '\r\n' (t_id, p_id, h_id, seat, start_stack, position)")
conn.commit()
print('Handplayer load completed at' + "--- %s seconds ---" % (time.time() - start_time))

'''create handaction table entries for entire list of tournaments'''
print('processing handaction data...')
for t in range(len(tupletourlist)):
    handlist = d4.gethandlist(tupletourlist[t])
    for hand in handlist:
        stagedict = d4.getstage(hand)
        h_id = d4.gethandid(hand)
        h_line, f_line, t_line, r_line, sd_line, sum_line = d4.getstagelines(hand)
        for line in range(2, h_line):
            action_at = stagedict.get(line)
            if d4.postRegex.search(hand[line]) is not None:
                mo = d4.postRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = -int(mo[0][3])
                action = mo[0][2]
                handactionWriter.writerow([h_id, p_id, action_at, action, value])

        for line in range(h_line + 2, sum_line):
            action_at = stagedict.get(line)
            if d4.foldRegex.search(hand[line]) is not None:
                mo = d4.foldRegex.findall(hand[line])
                p_id = playerdict.get(mo[0])
                value = None
                action = 'folds'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.checkRegex.search(hand[line]) is not None:
                mo = d4.checkRegex.findall(hand[line])
                p_id = playerdict.get(mo[0])
                value = None
                action = 'checks'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.betRegex.search(hand[line]) is not None:
                mo = d4.betRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = -int(mo[0][1])
                action = 'bets'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
                if mo[0][2] != '':
                    p_id = playerdict.get(mo[0][0])
                    value = 'y'
                    action = 'all-in'
                    handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.callRegex.search(hand[line]) is not None:
                mo = d4.callRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = -int(mo[0][1])
                action = 'calls'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
                if mo[0][2] != '':
                    p_id = playerdict.get(mo[0][0])
                    value = 'y'
                    action = 'all-in'
                    handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.raiseRegex.search(hand[line]) is not None:
                mo = d4.raiseRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = -int(mo[0][1])
                action = 'raises'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
                # print(hand[line])
                if mo[0][2] != '':
                    p_id = playerdict.get(mo[0][0])
                    value = 'y'
                    action = 'all-in'
                    handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.muckRegex.search(hand[line]) is not None:
                mo = d4.muckRegex.findall(hand[line])
                p_id = playerdict.get(mo[0])
                value = None
                action = 'mucks'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.uncalledRegex.search(hand[line]) is not None:
                mo = d4.uncalledRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][1])
                value = int(mo[0][0])
                action = 'uncalled bet'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.collectRegex.search(hand[line]) is not None:
                mo = d4.collectRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = int(mo[0][1])
                action = 'collects'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.showRegex.search(hand[line]) is not None:
                mo = d4.showRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = mo[0][1]
                action = 'shows'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.finishRegex.search(hand[line]) is not None:
                mo = d4.finishRegex.findall(hand[line])
                p_id = playerdict.get(mo[0][0])
                value = mo[0][1]
                action = 'finishes'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
            elif d4.winnerRegex.search(hand[line]) is not None:
                mo = d4.winnerRegex.findall(hand[line])
                p_id = playerdict.get(mo[0])
                value = '1st'
                action = 'wins'
                handactionWriter.writerow([h_id, p_id, action_at, action, value])
handactionFile.close()

print('Loading hand action data to database...')
cur.execute(
    "LOAD DATA LOCAL INFILE 'C:/Users/jkram/PycharmProjects/TM470/handaction.csv' INTO TABLE handaction FIELDS "
    "TERMINATED BY ',' LINES TERMINATED BY '\r\n' (h_id, p_id, action_at, action, value)")
conn.commit()
print('data load complete in' + "--- %s seconds ---" % (time.time() - start_time))
conn.close()

