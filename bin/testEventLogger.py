#! /usr/bin/env python

#
# LSST Data Management System
# Copyright 2008, 2009, 2010 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
#

#
from __future__ import with_statement
import sys
import os
import time
import datetime
import optparse
import traceback
import lsst.pex.harness.run as run
import lsst.pex.logging as logging
from lsst.pex.logging import Log, LogRec
from lsst.pex.exceptions import LsstException
from lsst.daf.base import PropertySet
import lsst.ctrl.events as events

usage = """Usage: %prog [-vqsd] [-V int] [broker]"""

desc = """send log messages as events to a log broker."""

cl = optparse.OptionParser(usage=usage, description=desc)
run.addAllVerbosityOptions(cl, "V")
cl.add_option("-r", "--runid", action="store", type="str",
              default="testEventLogger001", dest="runid", metavar="runid",
              help="runid to attach to messages")
cl.add_option("-S", "--slice", action="store", type="int", default=-1,
              dest="slice", metavar="id",
              help="slice ID to attach to messages")
cl.add_option("-t", "--stage", action="store", type="int", default=None,
              dest="stage", metavar="id",
              help="stage ID to attach to messages")
cl.add_option("-p", "--pipeline-name", action="store", type="str",
              dest="pipeline", metavar="name", default=None,
              help="pipeline name to attach to messages")
cl.add_option("-l", "--log-name", action="store", type="str",
              dest="logname", metavar="name", default=None,
              help="log name to send messages")
cl.add_option("-T", "--log-topic", action="store", type="str",
              default=events.EventLog.getLoggingTopic(),
              dest="logtopic", metavar="topic",
              help="event topic name (def: 'LSSTLogging')")
cl.add_option("-i", "--read-stdin", action="store_true", default=False,
              dest="stdin", help="read messages from standard input")

logger = Log(Log.getDefaultLog(), "showEvents")
VERB = logger.INFO-2


def main():
    """execute the testEventLogger script"""

    try:
        (cl.opts, cl.args) = cl.parse_args()
        Log.getDefaultLog().setThreshold(
            run.verbosity2threshold(cl.opts.verbosity, 0))

        props = {}
        if cl.opts.stage:
            props["stageId"] = cl.opts.stage
        if cl.opts.pipeline:
            props["pipeline"] = cl.opts.pipeline

        broker = None
        if len(cl.args) > 0:
            broker = cl.args[0]
        input = None
        if cl.opts.stdin:
            input = sys.stdin

        testEventLogger(broker, cl.opts.runid, cl.opts.slice, props,
                        input, cl.opts.logname, cl.opts.logtopic)
    except run.UsageError, e:
        print >> sys.stderr, "%s: %s" % (cl.get_prog_name(), e)
        sys.exit(1)
    except Exception, e:
        logger.log(Log.FATAL, str(e))
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)


def testEventLogger(broker, runid, sliceid, props, input=None, logname=None,
                    logtopic="LSSTLogging"):
    """
    test logging through a broker.  This will send a single message to a 
    the logger, and, if input is a list of string or and file input object,
    it will also send all input message in order.  If broker is None, the
    log messages will not got to any event broker, only to the screen.
    @param broker  the host running the event broker
    @param runid   the runid to assume for the log (ignored if broker is None)
    @param sliceid the sliceid to assume for the log (ignored if broker is None)
    @param props   a set of properties to attach to all messages
    @param input   if not None, messages to send, either in the form of
                      of a list of strings or a file object to read from
    @param logname the log name to send messages to 
    @param logtopic   the event topic to use (def: "LSSTLogging")
    """
    if broker:
        thresh = Log.getDefaultLog().getThreshold()
        setEventLogger(broker, runid, sliceid, thresh <= Log.DEBUG, logtopic)
        Log.getDefaultLog().setThreshold(thresh)
        logger.log(VERB, "Created event logger")
    else:
        logger.log(VERB, "Messages only going to screen")
        logger.setShowAll(Log.getDefaultLog().getThreshold() <= Log.DEBUG)

    if logname is None:
        logname = "showEvents"
    uselog = Log(Log.getDefaultLog(), logname)

    for key in props.keys():
        uselog.addPreamblePropertyString(key, props[key])

    testLogger(uselog, input)


def testLogger(log, input):
    """
    send test messages to the given logger
    @param log     the logger to send messages to
    @param input   if not None, messages to send, either in the form of
                      of a list of strings or a file object to read from
    """
    log.log(Log.INFO, "testing logger")

    if input:
        for line in input:
            log.log(Log.INFO, line.strip())


def setEventLogger(broker, runid, sliceid, verbose=False, topic="LSSTLogging"):

    topic = events.EventLog.getLoggingTopic()
    logger.log(VERB, "using event topic=%s" % topic)
    es = events.EventSystem.getDefaultEventSystem()
    es.createTransmitter(broker, topic)
    events.EventLog.createDefaultLog(runid, sliceid)

    deflog = Log.getDefaultLog()
    frmtr = logging.IndentedFormatter(verbose)
    deflog.addDestination(logging.cout, Log.DEBUG, frmtr)

if __name__ == "__main__":
    main()

