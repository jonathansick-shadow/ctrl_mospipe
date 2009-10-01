#! /usr/bin/env python
#
from __future__ import with_statement
import sys, os, time, datetime
import optparse, traceback
import lsst.pex.harness.run as run
from lsst.pex.logging import Log, LogRec
from lsst.pex.exceptions import LsstException
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events

usage = """Usage: %prog [-vqsd] [-V int] [-w seconds] [-S id] [-X hostlist|-H hostlist] broker [logname ...]"""

desc = """listen for and print events and their properties.

If no lognames are given on the command-line, messages with any log name
are printed.  A logname that ends with the a '*' character will match any
logname that begins with the string preceding the '*'. 
"""

cl = optparse.OptionParser(usage=usage, description=desc)
run.addAllVerbosityOptions(cl, "V")
cl.add_option("-w", "--wait-time", action="store", type="int", default=10, 
              dest="sleep", metavar="seconds",
              help="seconds to sleep when no events available (def: 10)")
cl.add_option("-S", "--slice", action="store", type="int", default=None, 
              dest="slice", metavar="id",
              help="restrict to given slice ID")
cl.add_option("-H", "--include-hosts", action="store", type="str",
              default=None, dest="inclhosts", metavar="hostlist",
              help="restrict to given hosts as comma-separated list")
cl.add_option("-m", "--min-importance", action="store", type="int",
              default=None, dest="minimport", metavar="importance",
              help="restrict to messages with this importance level or higher (e.g. FATAL=20, DEBUG < 0)")
cl.add_option("-M", "--max-importance", action="store", type="int",
              default=None, dest="maximport", metavar="importance",
              help="restrict to messages with this importance level or lower (e.g. FATAL=20, DEBUG < 0)")
cl.add_option("-X", "--exclude-hosts", action="store", type="str",
              default=None, dest="exclhosts", metavar="hostlist",
              help="ignor given hosts as comma-separated list")

logger = Log(Log.getDefaultLog(), "showEvents")
VERB = logger.INFO-2
timeoffset = time.time()

def main():
    """execute the watchLogs script"""

    try:
        (cl.opts, cl.args) = cl.parse_args()
        Log.getDefaultLog().setThreshold(
            run.verbosity2threshold(cl.opts.verbosity, 0))
        if cl.opts.inclhosts:
            cl.opts.inclhosts = cl.opts.inclhosts.split(',')
        if cl.opts.exclhosts:
            cl.opts.exclhosts = cl.opts.exclhosts.split(',')
        hosts = cl.opts.inclhosts
        if not hosts:
            hosts = cl.opts.exclhosts

        if cl.opts.minimport and cl.opts.maximport and \
           cl.opts.minimport > cl.opts.maximport:
            raise run.UsageError(
                "-m value (%i) should be less than -M value (%i)" %
                (cl.opts.minimport, cl.opts.maximport))

        watchLogs(cl.args[0], cl.args[1:], cl.opts.sleep,
                  cl.opts.slice, hosts, not bool(cl.opts.inclhosts),
                  cl.opts.minimport, cl.opts.maximport)

    except run.UsageError, e:
        print >> sys.stderr, "%s: %s" % (cl.get_prog_name(), e)
        sys.exit(1)
    except Exception, e:
        logger.log(Log.FATAL, str(e))
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)

def watchLogs(broker, lognames, sleep=10, sliceid=None,
              hosts=None, hostexclude=False,
              minimport=None, maximport=None):
    """
    print selected log messages
    @param broker    the host where the event broker is running
    @param lognames  a list (or space-delimited string) of log names to
                       listen for
    @parma sleep     seconds to sleep when no events are available
    @param sliceid   restrict to the given sliceid.  If none, print all slices
    @param hosts     restrict to the given list of host origins
    @param hostexclude  if True, the hosts lists are hosts to ignore 
                           messages from
    @param minimport restrict to messages with this importance level or higher.
                        If None (default), set no minimum restriction.
    @param maximport restrict to messages with this importance level or lower.
                        If None (default), set no maximum restriction.
    """
    if not isinstance(lognames, list):
        lognames = lognames.split()

    logger.log(VERB, "Watching for log names: " + ", ".join(lognames))

    rcvr = events.EventReceiver(broker, "LSSTLogging")
    listen(rcvr, sys.stdout, lognames, sleep, sliceid, hosts, hostexclude,
           minimport, maximport)

def listen(receiver, dest, lognames, sleep, sliceid=None, hosts=None,
           hostexclude=False, minimport=None, maximport=None):
           
    try:
        while True:
            if checkMessages(receiver, dest, lognames, sliceid,
                             hosts, hostexclude, minimport, maximport) == 0:
                time.sleep(sleep)
    except KeyboardInterrupt:
        logger.log(VERB, "KeyboardInterrupt: stopping event monitoring")

def checkMessages(receiver, dest, lognames, sliceid=None,
                  hosts=None, hostexclude=False,
                  minimport=None, maximport=None):
    thresh = logger.getThreshold()
    quiet = thresh >= logger.WARN
    loud = thresh <= VERB
    silent = thresh > logger.FATAL
    count = 0

    likenames = map(lambda x: x[:-1],
                   filter(lambda y: y.endswith('*'), lognames))
    names = filter(lambda y: not y.endswith('*'), lognames)

    while True:
        event = receiver.receive(0)
        if not event:
            break
        ts = time.time()
        name = event.getString("LOG", "")
        if lognames and name not in names and \
           not bool(filter(lambda x: name.startswith(x), likenames)):
            continue
        if lognames and sliceid and event.getInt("sliceId", -2) != sliceid:
            continue
        if hosts:
            host = event.getString('hostId', "")
            if hostexclude:
                if bool(filter(lambda x: host.startswith(x), hosts)):
                    continue
            else:
                if not bool(filter(lambda x: host.startswith(x), hosts)):
                    continue

        level = event.getInt("LEVEL", 0)
        if minimport is not None and level < minimport:  continue
        if maximport is not None and level > maximport:  continue
        
        date = str(datetime.datetime.utcfromtimestamp(ts))
        ts -= timeoffset
        if event.exists("TIMESTAMP"):
            ts = event.get("TIMESTAMP").nsecs() / 1.0e9
            date = str(datetime.datetime.utcfromtimestamp(ts))
            ts -= timeoffset
        if event.exists("DATE"):
            date = event.get("DATE")

        lev = ""
        if level >= logger.FATAL:
            lev = "FATAL "
        elif level >= logger.WARN:
            lev = "WARN "
        elif level < logger.INFO:
            lev = "DEBUG "
        count += 1
        
        if silent:
            continue
        for comm in event.getArrayString("COMMENT"):
            print "%s%s (%f): %s" % (lev, name, ts, comm)
        if not quiet:
            print event.toString()

    return count

if __name__ == "__main__":
    main()
        
