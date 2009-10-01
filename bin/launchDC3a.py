#! /usr/bin/env python
#
from __future__ import with_statement
import sys, os, time
import optparse, traceback
import lsst.pex.harness.run as run
from lsst.pex.logging import Log, LogRec
from lsst.pex.policy import Policy
from lsst.pex.exceptions import LsstException
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events

usage = """usage: %prog [-vqsd] [-V int] [-L lev] [-r dir] [-e script] [-C coll] [-m maxvisits] [-t cfht|sim] dc3pipe_policy_file runId [ visitListFile ... ]
"""
desc = """Launch all or parts of the DC3a Alert Production according to a
given production policy file."""

cl = optparse.OptionParser(usage=usage, description=desc)
run.addVerbosityOption(cl, dest="pipeverb")
cl.add_option("-m", "--max-visits", action="store", type="int",
              dest="maxvisits", default=-1,
              help="maximum number of visits to trigger")
cl.add_option("-V", "--verbosity", type="int", action="store",
              dest="verbosity", default=0, metavar="int",
         help="orca verbosity level (0=normal, 1=debug, -1=quiet, -3=silent)")
cl.add_option("-r", "--policyRepository", type="string", action="store",
              dest="repos", default=None, metavar="dir",
              help="directory containing policy files")
cl.add_option("-e", "--envscript", action="store", dest="envscript",
              default=None, metavar="script",
          help="an environment-setting script to source on pipeline platform")
cl.add_option("-d", "--debug", action="store_const", const=1, 
              dest="verbosity", help="print maximum amount of messages")
cl.add_option("-v", "--verbose", action="store_const", const=1,
              dest="verbosity", help="same as -d")
cl.add_option("-q", "--quiet", action="store_const", const=-1,
              dest="verbosity", help="print only warning & error messages")
cl.add_option("-s", "--slew-time", action="store", type="int", default=5, 
             dest="slewtime", help="telescope slew time in seconds (def: 5)")
cl.add_option("-x", "--exposure-time", action="store", type="int", default=15, 
              dest="exptime", help="exposure time in seconds (def: 15)")
cl.add_option("-t", "--data-type", action="store", default="cfht", 
              dest="datatype",
              help="type of data in given visit files; choices: cfht|sim; " +
              "minimum match, case-insensitive; def: cfht")
cl.add_option("-C", "--collections", action="store", default=None, 
              dest="colls", help="a list of the datset collections names (support: D1|D2|D3|D4)")

dc3apkg   = "ctrl_dc3pipe"
pkgdirvar = dc3apkg.upper() + "_DIR"
loggingEventTopic = events.EventLog.getLoggingTopic()
waitLogName = "harness.pipeline.visit.stage.handleEvents.eventwait"
setuptime = 3000    # seconds
shortsetuptime = 30 # seconds
datatypes = { "cfht": "datatypePolicy/cfhtDataTypePolicy.paf",
              "sim":  "datatypePolicy/simDataTypePolicy.paf"   }
datatypes['d1'] = datatypes['cfht']
datatypes['d2'] = datatypes['cfht']
datatypes['d3'] = datatypes['cfht']
datatypes['d4'] = datatypes['cfht']

def main():
    "execute the launchDC3 script"

    logger = Log(Log.getDefaultLog(), "launchDC3")
    try:
        (cl.opts, cl.args) = cl.parse_args()
        Log.getDefaultLog().setThreshold(-10 * cl.opts.verbosity)

        if cl.opts.pipeverb is None:
            cl.opts.pipeverb = "trace"

        t = filter(lambda x: x.startswith(cl.opts.datatype.lower()),
                   datatypes.keys())
        if len(t) > 1:
            raise ValueError("Ambiguous data type name: " + cl.opts.datatype)
        if len(t) == 0:
            raise ValueError("Unrecognized data type name: "+ cl.opts.datatype)
        cl.opts.datatype = datatypes[t[0]]

        colls = []
        # parse the collection names
        if cl.opts.colls is not None:
            colls = cl.opts.colls.split(',')

        launchDC3a(cl.args[0], cl.args[1], cl.args[2:], colls, cl.opts, logger)
        
    except run.UsageError, e:
        print >> sys.stderr, "%s: %s" % (cl.get_prog_name(), e)
        sys.exit(1)
    except Exception, e:
        logger.log(Log.FATAL, str(e))
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)
        

def launchDC3a(policyFile, runid, visitFiles, colls, opts, logger):

    if not os.environ.has_key(pkgdirvar):
        raise pexExcept.LsstException("%s env. var not set (setup %s)"
                                      % (pkgdirvar, dc3apkg))
    if opts.repos is None:
        opts.repos = os.path.join(os.environ[pkgdirvar], "pipeline")

    policy = Policy.createPolicy(policyFile, opts.repos)
    broker = policy.get("eventBrokerHost")
    logger.log(Log.DEBUG, "Using event broker on %s" % broker)

    recvr = events.EventReceiver(broker, loggingEventTopic)
    
    runOrca(policyFile, runid, opts, logger)

    waitForReady(policy, runid, recvr, opts.pipeverb, logger)

    runEventGen(policy, visitFiles, colls, opts, broker, logger)

def runOrca(policyFile, runid, opts, logger):
    cmdopts = ""
    if opts.repos is not None:
        cmdopts += " -r %s" % opts.repos
    if opts.envscript is not None:
        cmdopts += " -e %s" % opts.envscript
    if opts.verbosity != 0:
        cmdopts += " -V %s" % opts.verbosity
    if opts.pipeverb is not None:
        cmdopts += " -L %s" % opts.pipeverb
        
    cmd = "orca.py%s %s %s" % (cmdopts, policyFile, runid)
    try:
        sysexec(cmd, logger)
    except OSError, e:
        raise LsstException("orca.py failed: " + str(e))

def waitForReady(policy, runid, eventrcvr, logverb, logger):
    """
    attempt to wait until all pipelines are configured and running before
    sending event data.
    """
    # This implimentation tries to wait until all pipelines have reached
    # the point of waiting for their first event

    # determine whether the pipeline verbosity is enough to get the
    # particular "ready" signals we will be looking for
    prodthresh = None
    if logverb is not None:
        prodthresh = run.verbosity2threshold(logverb)
    if prodthresh is None and policy.exists("logThreshold"):
        prodthresh = policy.get("logThreshold")

    timeout = setuptime

    pldescs = policy.get("pipelines")
    names = pldescs.policyNames(True)
    pipelines = []
    for pl in names:
        plpol = pldescs.getPolicy(pl)
        if not plpol.getBool("launch"):
            continue

        if prodthresh is None or prodthresh > -1:
            config = plpol.getPolicy("configuration")
            if config.exists("execute"):
                config = config.getPolicy("execute")
            if config.exists("logThreshold") and \
               config.getInt("logThreshold") > -1:
                logger.log(Log.WARN, "%s pipeline's logging not verbose enough to track its readiness" % pl)
                continue

        pipelines.append(pl)
        logger.log(Log.DEBUG,
                   "Waiting for the %s pipeline to be ready..." % pl)

    if "IPSD" not in pipelines:
        timeout = shortsetuptime # seconds

    if len(pipelines) > 0:
        logger.log(Log.INFO,
                   "Waiting for pipelines to setup (this can take a while)...")

        tick = time.time()
        while len(pipelines) > 0:
            waittime = 1000 * (timeout - int(round(time.time()-tick)))
            if waittime > 0:
#                waitprops = eventrcvr.receive(waittime)
                waitprops = eventrcvr.matchingReceive("LOG", waitLogName,
                                                      waittime)
            else:
                waitprops = None

            if waitprops is None:
                LogRec(logger, Log.WARN) \
                  << "Have yet to hear back from the following pipelines: " +\
                      ", ".join(pipelines) \
                  << "Proceeding to send visit events" << LogRec.endr
                break
            if waitprops.getString("STATUS", "") == "start" and \
               waitprops.getString("runId", "") == runid:
                pipename = waitprops.getString("pipeline", "unknown")
                if pipename in pipelines:
                    pipelines.remove(pipename)
#                pipelines.pop(0)
                logger.log(Log.DEBUG, "%s is ready" % pipename)

    else:
        LogRec(logger, Log.WARN) \
                       << "Unable to detect when pipelines are ready" \
                       << "Proceeding to send visit events in %d seconds" % \
                          shortsetuptime \
                       << LogRec.endr
        time.sleep(shortsetuptime)

    return

def runEventGen(policy, visitFiles, colls, opts, broker, logger):

    stopEventTopic = None
    if policy.exists("shutdownTopic"):
        stopEventTopic = policy.get("shutdownTopic")

    try:
        for file in visitFiles:
            cmdopts = ""
            if opts.verbosity is not None:
                cmdopts += " -V %i" % opts.verbosity
            if opts.maxvisits >= 0:
                cmdopts += " -m %i" % opts.maxvisits
            datatypePolicy = os.path.join(os.environ[pkgdirvar], "pipeline",
                                          opts.datatype)
            
            cmd = "eventFromFitsfileList.py%s -b %s %s %s" % \
                  (cmdopts, broker, file, datatypePolicy)
            if opts.exptime is not None:
                cmd += " %i" % opts.exptime
            if opts.slewtime is not None:
                cmd += " %i" % opts.slewtime

            try:
                sysexec(cmd, logger)
            except OSError, e:
                raise LsstException("eventFromFitsfileList.py failed: "+str(e))

        for coll in colls:
            if coll.lower() not in datatypes.keys():
                logger.log(Log.WARN,
                           "Unrecognized collection name: %s (skipping...)" %
                           coll)
                continue

            cmdopts = ""
            if opts.verbosity is not None:
                cmdopts += " -V %i" % opts.verbosity
            if opts.maxvisits >= 0:
                cmdopts += " -m %i" % opts.maxvisits
            datatypePolicy = os.path.join(os.environ[pkgdirvar], "pipeline",
                                          datatypes[coll.lower()])

            cmd = "eventGeneratorForCFHT.py%s -b %s %s %s" % \
                  (cmdopts, broker, coll.upper(), datatypePolicy)
            if opts.exptime is not None:
                cmd += " %i" % opts.exptime
            if opts.slewtime is not None:
                cmd += " %i" % opts.slewtime

            try:
                sysexec(cmd, logger)
            except OSError, e:
                raise LsstException("eventGeneratorForCFHT.py failed: "+str(e))
    finally:
        pass
#        if stopEventTopic is not None:
#            trx = events.EventTransmitter(broker, stopEventTopic)
#            trx.publish(PropertySet())

    return


def sysexec(cmd, logger):
    if logger is not None:
        logger.log(logger.DEBUG, "Executing: %s" % cmd)
    cmd = cmd.split()
    exno = os.spawnvp(os.P_WAIT, cmd[0], cmd);
    if exno != 0:
        raise OSError("Command exited with code %d" % (exno >> 8))

if __name__ == "__main__":
    main()
