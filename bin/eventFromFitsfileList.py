#!/usr/bin/env python

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


import os
import sys
import re
import traceback
import glob
import imp
import time
import eups
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.pex.policy as pexPolicy
import lsst.pex.logging as pexLog
import lsst.daf.base as dafBase
import lsst.ctrl.events as ctrlEvents
from lsst.pex.harness import run
from lsst.ctrl.dc3pipe.MetadataStages import transformMetadata, validateMetadata
usage = """Usage: %prog [-dvqs] [-V lev] [-b host] [-t topic] visitfile policyfile [exptime] [slewtime]"""
desc = """Generate events for the IPSD (and MOPS) pipeline by reading a list of visit
directories and extracting the relevant information from the FITS files
therein.

The two optional parameters are the exposure time of each exposure in a visit
(assumed constant) and the average slew time of the telescope. Both are in 
seconds. The default to 15 seconds and 5 seconds respecively.

The script sends one event for the first visit exposure, waits exptime seconds
and then sends an event for the second exposure. At that point, it waits 
(exptime + slewtime) seconds before passing to the next visit.

The input directory list is a simple text file listing visit directories one 
per line. Comments start with a '#' and are ignored. It is assumed that the 
name of each directory in the file is a valid visitId. Also it is assumed that 
each directory has the following structure:
    visitId/
            0/
              raw-<visitId>-e000-c<ccdId>-a<ampId>.fits
            1/
              raw-<visitId>-e001-c<ccdId>-a<ampId>.fits
"""

# Import EventFromInputfile from eventFromFitsfile.py so that we do not have to
# re-write that one. We do not use the imputil modules since it is deprecated as
# of Python 2.6 and removed in Python 3.0.
# From http://docs.python.org/3.0/library/imp.html
# Fast path: see if the module has already been imported.
if('eventFromFitsfile' in sys.modules):
    import eventFromFitsfile
else:
    thisDir = this_dir = os.path.dirname(os.path.realpath(__file__))
    fp, pathname, description = imp.find_module('eventFromFitsfile', [thisDir, ])
try:
    eventFromFitsfile = imp.load_module('eventFromFitsfile', fp, pathname,
                                        description)
finally:
    # Since we may exit via an exception, close fp explicitly.
    if fp:
        fp.close()

# Constants
EXP_TIME = 15.
SLEW_TIME = 5.
ROOT_EVENT_TOPIC = 'triggerImageprocEvent'
EVENT_BROKER = 'lsst4.ncsa.uiuc.edu'

logger = pexLog.Log(pexLog.Log.getDefaultLog(),
                    "dc3pipe.eventFromFitsfileList")
visitCount = 0


def EventFromInputFileList(inputfile,
                           datatypePolicy,
                           expTime=EXP_TIME,
                           slewTime=SLEW_TIME,
                           maxvisits=-1,
                           rootTopicName=ROOT_EVENT_TOPIC,
                           hostName=EVENT_BROKER,
                           metadataPolicy=None):
    """
    Generate events for the IPSD (and MOPS) pipeline by reading a list of visit
    directories and extracting the relevant information from the FITS files 
    therein.

    The two optional parameters are the exposure time of each exposure in a 
    visit (assumed constant) and the average slew time of the telescope. Both 
    are in seconds. The default to 15 seconds and 5 seconds respecively.

    The script sends one event for the first visit exposure, waits <exp time> 
    sec and then sends an event for the second exposure. At that point, it 
    waits (<exp time> + <slew time>) sec before passing to the next visit.

    The input directory list is a simple text file listing visit directories 
    one per line. Comments start with a '#' and are ignored. It is assumed 
    that the name of each directory in the file is a valid visitId. Also it 
    is assumed that each directory has the following structure:
        visitId/
                0/
                  raw-<visitId>-e000-c<ccdId>-a<ampId>.fits
                1/
                  raw-<visitId>-e001-c<ccdId>-a<ampId>.fits

    @param inputfile        name of the directory list file.
    @param datatypePolicy   policy file for the input data.
    @param expTime          assumed exposure per visit in seconds (defaults 
                               to 15 seconds for DC3).
    @param slewTime         assumed average slew time in seconds (defaults 
                               to 5 seconds for DC3). 
    @param rootTopicName    root name for the event's topic. The final topic 
                               will be rootTopicName+'0' or rootTopicName+'1'
                               depending on whether the event refers to the
                               first or second image of the visit.
    @param hostName         hostname of the event broker.
    @param metadataPolicy   policy defining the event metadata types

    @return None
    """
    global visitCount

    # Create a metadata policy object.
    if metadataPolicy is None:
        mpf = pexPolicy.DefaultPolicyFile("ctrl_dc3pipe",
                                          "dc3MetadataPolicy.paf", "pipeline")
        metadataPolicy = pexPolicy.Policy.createPolicy(mpf,
                                                       mpf.getRepositoryPath())

    # Covenience function.
    def sendEvent(f):
        return(eventFromFitsfile.EventFromInputfile(f,
                                                    datatypePolicy,
                                                    metadataPolicy,
                                                    rootTopicName,
                                                    hostName))

    f = open(inputfile)
    for line in f:
        dirName = line.strip()
        if(line.startswith('#')):
            continue

        visitCount += 1
        if maxvisits >= 0 and visitCount > maxvisits:
            logger.log(logger.INFO,
                       "Maximum visit count reached (%s); quitting." %
                       maxvisits)
            return

        # Get the list of amp FITS files in each dir.
        fileList0 = glob.glob(os.path.join(dirName, '0', '*.fits'))
        fileList1 = glob.glob(os.path.join(dirName, '1', '*.fits'))

        # Simple sanity check.
        if(len(fileList0) != len(fileList1)):
            pexLog.Trace('dc3pipe.eventfrominputfilelist', 1,
                         'Skipping %s: wrong file count in 0 and 1'
                         % (dirName))
            continue

        # Now we just trust that the i-th file in 0 corresponds to the i-th file
        # in 1... Fortunately, we only need to send one event per image
        # directory, since all images there are one MEF split into individual
        # amps.
        sendEvent(fileList0[0])
        # Sleep some.
        time.sleep(expTime)
        # Next event.
        sendEvent(fileList1[0])
        # Sleep expTime + slewTime.
        time.sleep(expTime + slewTime)
    f.close()
    return


def defineCmdLine():
    cl = eventFromFitsfile.defineCmdLine(usage, desc)
    cl.add_option("-m", "--max-visits", action="store", type="int",
                  dest="maxvisits", default=-1,
                  help="maximum number of visits to trigger")
    return cl


def main(cmdline):
    """
    run the script with the given command line
    @param cmdline   an OptionParser instance with command-line options defined
    """
    cl = cmdline
    (cl.opts, cl.args) = cl.parse_args()
    pexLog.Log.getDefaultLog().setThreshold(
        run.verbosity2threshold(cl.opts.verb, 0))

    if len(sys.argv) < 3:
        raise run.UsageError("Missing arguments")

    inputDirectoryList = cl.args[0]
    datatypePolicy = pexPolicy.Policy.createPolicy(cl.args[1])
    expTime = EXP_TIME
    slewTime = SLEW_TIME
    if len(cl.args) > 2:
        expTime = float(cl.args[2])
    if len(cl.args) > 3:
        slewTime = float(cl.args[3])

    metadataPolicy = None
    if cl.opts.mdpolicy is not None:
        metadataPolicy = pexPolicy.Policy.createPolicy(metadataPolicy)

    EventFromInputFileList(inputDirectoryList, datatypePolicy, expTime,
                           slewTime, cl.opts.maxvisits, cl.opts.topic,
                           cl.opts.broker, metadataPolicy)


if __name__ == "__main__":
    try:
        cl = defineCmdLine()
        main(cl)
    except run.UsageError, e:
        print >> sys.stderr, "%s: %s" % (cl.get_prog_name(), e)
        sys.exit(1)
    except Exception, e:
        logger.log(logger.FATAL, str(e))
        traceback.print_exc(file=sys.stderr)
        sys.exit(2)


