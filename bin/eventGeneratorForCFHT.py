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


import os, sys, re, traceback
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
usage = """Usage: %prog [-dvqs] [-V lev] [-b host] [-t topic] <D1|D2|D3|D4|ALL> <policy_file> [<exp time>] [<slew time>]"""
desc = """Generate events for the IPSD (and MOPS) pipeline by extracting the relevant 
information from the FITS files in the standard DC3 CFHT subdirectories.

Users specify whether they want events generated for all fields or only for a
subset. In the latter case, they can specify the name of the subset: D1 through
D4. Multiple subset can be specified as a space separated list in quotes (e.g.
'D1 D3'). 'ALL' is shorthand for ['D1', 'D2', 'D3', 'D4'].

A second required parameter is the path to the data policy file for the input
FITS files.

The two optional parameters are the exposure time of each exposure in a visit
(assumed constant) and the average slew time of the telescope. Both are in 
seconds. The default to 15 seconds and 5 seconds respecively.

The script sends one event for the first visit exposure, waits exptime seconds
and then sends an event for the second exposure. At that point, it waits 
(exptime + slewtime) seconds before passing to the next visit.
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
    fp, pathname, description = imp.find_module('eventFromFitsfile', [thisDir,])
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
ROOT_DIR = '/lsst/images/repository/raw'
SUBSETS = ['ALL', 'D1', 'D2', 'D3', 'D4']
ROOT_EVENT_TOPIC = 'triggerImageprocEvent'
EVENT_BROKER = 'lsst4.ncsa.uiuc.edu'

logger = pexLog.Log(pexLog.Log.getDefaultLog(),
                    "dc3pipe.eventGeneratorForCFHT")
visitCount = 0

def EventFromInputSubsets(subsets, 
                          datatypePolicy, 
                          expTime=EXP_TIME,
                          slewTime=SLEW_TIME,
                          maxvisits=-1,
                          rootTopicName=ROOT_EVENT_TOPIC,
                          hostName=EVENT_BROKER,
                          metadataPolicy=None):
    """
    Generate events for the IPSD (and MOPS) pipeline by extracting the relevant
    information from the FITS files in the standard DC3 CFHT subdirectories.

    Users specify whether they want events generated for all fields or only for
    a subset. In the latter case, they can specify the name of the subset: D1 
    through D4. Multiple subset can be specified as a space separated list in 
    quotes (e.g. 'D1 D3'). 'ALL' is shorthand for ['D1', 'D2', 'D3', 'D4'].

    A second required parameter is the path to the data policy file for the 
    input FITS files.

    The two optional parameters are the exposure time of each exposure in a 
    visit (assumed constant) and the average slew time of the telescope. Both 
    are in seconds. The default to 15 seconds and 5 seconds respecively.

    The script sends one event for the first visit exposure, waits expTime 
    seconds and then sends an event for the second exposure. At that point, 
    it waits (expTime + slewTime) seconds before passing to the next visit.
    
    @param subsets          list of subset names (e.g. ['D1', 'D2']). 'ALL'
                               is shorthand for ['D1', 'D2', 'D3', 'D4'].
    @param datatypePolicy   Policy file for the input data.
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
    
    if('ALL' in subsets):
        subsets = ['D1', 'D2', 'D3', 'D4']
    
    for subset in subsets:
        dirName = os.path.join(ROOT_DIR, subset)
        subDirNames = os.listdir(dirName)
        
        for subDirName in subDirNames:
            subDirPath = os.path.join(dirName, subDirName)
            if(not os.path.isdir(subDirPath)):
                continue

            visitCount += 1
            if maxvisits >= 0 and visitCount > maxvisits:
                logger.log(logger.INFO,
                           "Maximum visit count (%s) reached; quitting." %
                           maxvisits)
                return 
            
            # Get the list of amp FITS files in each dir.
            fileList0 = glob.glob(os.path.join(subDirPath, '0', '*.fits'))
            fileList1 = glob.glob(os.path.join(subDirPath, '1', '*.fits'))
            
            # Simple sanity check.
            if(len(fileList0) != len(fileList1)):
                logger.log(logger.WARN,
                           'Skipping %s: wrong file count in 0 and 1' \
                           % os.path.dirname(fileList0) )
                continue
            
            # Now we just trust that the i-th file in 0 corresponds to the 
            # i-th file in 1... Fortunately, we only need to send one event 
            # per image directory, since all images there are one MEF split 
            # into individual amps.
            sendEvent(fileList0[0])
            # Sleep some.
            time.sleep(expTime)
            # Next event.
            sendEvent(fileList1[0])
            # Sleep expTime + slewTime.
            time.sleep(expTime + slewTime)
    return
    
def defineCmdLine():
    cl = eventFromFitsfile.defineCmdLine(usage, desc)
    cl.add_option("-m", "--max-visits", action="store", type="int",
                  dest="maxvisits", default=-1,
                  help="maximum number of visits to trigger")
    return cl

def main(cmdline):
    import sets
    
    """
    run the script with the given command line
    @param cmdline   an OptionParser instance with command-line options defined
    """
    cl = cmdline
    (cl.opts, cl.args) = cl.parse_args()
    pexLog.Log.getDefaultLog().setThreshold( \
        run.verbosity2threshold(cl.opts.verb, 0))

    if len(cl.args) < 3:
        raise run.UsageError("Missing arguments")
    
    subsets = [x.upper() for x in cl.args[0].split()]

    # Make sure thet the subsets make sense.
    if(not sets.Set(subsets).issubset(sets.Set(SUBSETS))):
        raise run.UsageError('subsets must be a combination of %s' 
                             % ', '.join(SUBSETS) )
    
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
    
    EventFromInputSubsets(subsets, datatypePolicy, expTime, 
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
    
