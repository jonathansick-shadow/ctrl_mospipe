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
import re, sys, os, os.path, shutil, subprocess
import optparse, traceback
from lsst.pex.logging import Log
from lsst.pex.policy import Policy
from lsst.pex.exceptions import LsstCppException

usage = """usage: %prog [-vqs] [-V int] [-r reposDir ] [-p dc3pipe_policy_file] [-i runId] [node ...]

Kill the pipelines running on a give set of head nodes.
"""

cl = optparse.OptionParser(usage)
cl.add_option("-V", "--verbosity", type="int", action="store",
              dest="verbosity", default=0, metavar="int",
              help="verbosity level (0=normal, 1=debug, -1=quiet, -3=silent)")
cl.add_option("-v", "--verbose", action="store_const", const=1,
              dest="verbosity",
              help="print extra messages")
cl.add_option("-q", "--quiet", action="store_const", const=-1,
              dest="verbosity",
              help="print only warning & error messages")
cl.add_option("-s", "--silent", action="store_const", const=-3,
              dest="verbosity",
              help="print only warning & error messages")
cl.add_option("-n", "--show", action="store_true", default=False,
              dest="showOnly",
              help="only show the kill commands that would be run")
cl.add_option("-p", "--production-policy", action="store", dest="prodpol", 
              default=None, metavar="policy_file",
              help="the dc3pipe production policy file used to launch the pipelines")
cl.add_option("-l", "--platform-policy", action="store", dest="platpol", 
              default=None, metavar="policy_file",
              help="the platform policy file used to launch a pipeline")
cl.add_option("-i", "--runid", action="store", dest="runid", 
              default="", metavar="runid",
              help="restrict the kill to pipelines running with this runid")
cl.add_option("-r", "--repository-dir", action="store", dest="repos", 
              default=None, metavar="dir",
           help="assume the given policy repository directory (for -p and -l)")

# command line results
cl.opts = {}
cl.args = []

pkgdirvar = "CTRL_DC3PIPE_DIR"
defDomain = ".ncsa.uiuc.edu"
remkill = "killpipe.sh"

def createLog():
    log = Log(Log.getDefaultLog(), "dc3pipe")
    return log

def setVerbosity(verbosity):
    logger.setThreshold(-10 * verbosity)  

logger = createLog()

def main():
    try:
        (cl.opts, cl.args) = cl.parse_args();
        setVerbosity(cl.opts.verbosity)

        nodes = []
        if cl.opts.prodpol is not None:
            policy = Policy.createPolicy(cl.opts.prodpol, False)
            repos = cl.opts.repos
            if repos is None:
                try:
                    repos = getRepositoryDir(policy)
                except:
                    pass
            if repos is not None:
                policy.loadPolicyFiles(repos)

            nodes.extend(getHeadNodes(policy))
            
        if cl.opts.platpol is not None:
            policy = Policy.createPolicy(cl.opts.platpol)
            nodes.extend(getHeadNode(policy))
            
        nodes.extend(cl.args)
        logger.log(Log.DEBUG, "Killing pipelines on " + ", ".join(nodes))

        remcmd = "%s %s" % \
            (os.path.join(os.environ[pkgdirvar], "bin", remkill),cl.opts.runid)
        remcmd = remcmd.strip()

        for node in nodes:
            cmd = ("ssh", node, remcmd)
            logger.log(Log.INFO, "executing: %s %s '%s'" % cmd)
            if not cl.opts.showOnly and subprocess.call(cmd) != 0:
                logger.log(Log.WARN, "Failed to kill processes on " + node)

    except:
        tb = traceback.format_exception(sys.exc_info()[0],
                                        sys.exc_info()[1],
                                        sys.exc_info()[2])
        logger.log(Log.FATAL, tb[-1].strip())
        logger.log(Log.DEBUG, "".join(tb[0:-1]).strip())
        sys.exit(1)

    sys.exit(0)

def getHeadNodes(prodpolicy, file=None):
    """return the head from a platform policy.
    @param prodpolicy   a production policy object
    @param file         the file where this was loaded from
    """
    pipepol = prodpolicy.get("pipelines")
    pipelines = pipepol.policyNames(True)

    nodes = []
    for pipeline in pipelines:
        ppol = pipepol.get(pipeline)
        if ppol.get("launch", True):
            try: 
                platform = ppol.get("platform")
                if platform is not None:
                    nodes.extend(getHeadNode(platform, file, pipeline))
            except LsstCppException, e:
                msg = \
                  "Pipeline policy for %s is missing platform item" % pipeline
                if file is not None:
                    msg += " via %s" % file
                logger.log(Log.WARN, msg)
            
    return nodes

def getHeadNode(platpolicy, file=None, pipeline=None):
    """return the head from a platform policy.
    @param platpolicy   a policy object
    @param file         the file where this was loaded from
    @param pipeline     then name of the pipeline
    """
    try: 
        plnodes = platpolicy.getArray("deploy.nodes")
        if plnodes is None or len(plnodes) == 0:  return []
        return [plnodes[0].split(':')[0]]
    except LsstCppException, e:
        msg = "Platform policy is missing platform item"
        if pipeline is not None:
            msg += " for %s" % pipeline
        if file is not None:
            msg += " via %s" % file
        logger.log(Log.WARN, msg)

    return []

def getRepositoryDir(prodpolicy):
    """
    return the full path version of the value of the repositoryDirectory
    policy item from a production-level policy file
    """
    dir = prodpolicy.get("repositoryDirectory")
    envre = re.compile(r'\$(\w+)')
    next = dir
    var = envre.search(next)
    while var is not None:
        varname = var.group(1)
        next = next[var.end(1):]
        if os.environ.has_key(varname):
            dir = re.sub(r'\$'+varname, os.environ[varname], dir)
        var = envre.search(next)

    return dir
        

if __name__ == "__main__":
    main()
