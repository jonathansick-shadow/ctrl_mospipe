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
import optparse
import traceback
import eups
import lsst.afw.image as afwImage
import lsst.afw.math as afwMath
import lsst.pex.policy as pexPolicy
import lsst.pex.logging as pexLog
import lsst.daf.base as dafBase
import lsst.ctrl.events as ctrlEvents
from lsst.pex.harness import run
from lsst.ctrl.mospipe.MetadataStages import transformMetadata, validateMetadata

usage = """Usage: %prog [-dvqs] [-V lev] [-b host] [-t topic] FITSfile policyfile"""
desc = """Send an incoming visit event to instruct the alert production to process
a given FITS file.  The data for the event is extracted from the FITS file.
The given policy file controls the transformation of the FITS metadata into
the event metadata where different input data collections (CFHT, simulated,
etc.) will require different policies.  See the
$CTRL_DC3PIPE/pipeline/datatypePolicy directory for samples.  
"""

logger = pexLog.Log(pexLog.Log.getDefaultLog(), "mospipe.eventFromFitsfile")
exposureCount = 0
VERB3 = run.verbosity2threshold("verb3", logger.INFO-3)


def defineCmdLine(usage=usage, description=desc):
    cl = optparse.OptionParser(usage=usage, description=description)
    run.addAllVerbosityOptions(cl, "V", "verb")
    cl.add_option("-b", "--broker", action="store", dest="broker",
                  default="newfield.as.arizona.edu", help="event broker host")
    cl.add_option("-t", "--topic", action="store", dest="topic",
                  default="triggerImageprocEvent", help="event topic name")
    cl.add_option("-M", "--metadata-policy", action="store", dest="mdpolicy",
                  default=None,
                  help="policy file defining the event metadata types")
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

    mdPolicyFileName = cl.opts.mdpolicy
    if mdPolicyFileName is None:
        mpf = pexPolicy.DefaultPolicyFile("ctrl_mospipe",
                                          "mosEventMetadataPolicy.paf",
                                          "pipeline")
        metadataPolicy = pexPolicy.Policy.createPolicy(mpf,
                                                       mpf.getRepositoryPath())
    else:
        metadataPolicy = pexPolicy.Policy.createPolicy(mdPolicyFileName)

    dataPolicy = pexPolicy.Policy.createPolicy(cl.args[1])

    if not EventFromInputfile(cl.args[0], dataPolicy, metadataPolicy,
                              cl.opts.topic, cl.opts.broker):
        # EventFromInputfile will print error message
        sys.exit(3)


def EventFromInputfile(inputfile,
                       datatypePolicy,
                       metadataPolicy,
                       topicName='triggerImageprocEvent',
                       hostName='newfield.as.arizona.edu'):
    """generate a new file event for a given FITS file
    @param inputfile       the name of the FITS file
    @param datatyepPolicy  the policy describing the metadata transformation
    @param metadataPolicy  the policy describing the event metadata types
    @param topicName       the name of the topic to send event as
    @param hostName        the event broker hostname
    """
    global exposureCount
    exposureCount += 1

    # For mosphot, inputfile is a .fits file on disk
    metadata = afwImage.readMetadata(inputfile)
#    logger.log(logger.INFO,"Original metadata:\n" + metadata.toString())

    # First, transform the input metdata
    transformMetadata(metadata, datatypePolicy, metadataPolicy, 'Keyword')

    # To be consistent...
    if not validateMetadata(metadata, metadataPolicy):
        logger.log(logger.FATAL, 'Unable to create event from %s' % inputfile)

    # Create event policy, using defaults from input metadata
    event = dafBase.PropertySet()
    event.copy('exposureId', metadata, 'exposureId')
    event.copy('datasetId', metadata, 'datasetId')
    event.copy('filter', metadata, 'filter')
    event.copy('expTime', metadata, 'expTime')
    event.copy('ra', metadata, 'ra')
    event.copy('decl', metadata, 'decl')
    event.copy('equinox', metadata, 'equinox')
    event.copy('airmass', metadata, 'airmass')
    event.copy('dateObs', metadata, 'dateObs')

    eventTransmitter = ctrlEvents.EventTransmitter(hostName, topicName)

    logger.log(logger.INFO,
               'Sending event for %s' % os.path.basename(inputfile))
    if logger.sends(logger.DEBUG):
        logger.log(logger.DEBUG, "Data Event data:\n%s" % event.toString())
    elif logger.sends(VERB3):
        logger.log(VERB3,
                   "Event data: datasetId=%s; ra=%f, dec=%f" %
                   (event.get("datasetId"), event.get("ra"), event.get("decl")))

    eventTransmitter.publish(event)

    return True


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

