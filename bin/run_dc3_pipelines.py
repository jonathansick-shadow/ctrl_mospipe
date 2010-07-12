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

"""
run_dc3_pipelines.py

Checkout the IPSD, MOPS and DC3 pipeline description/policy files form SVN, edit
the DC3 policy to use our local files and start a run.
"""
import os
import sets
import shutil
import sys
import traceback

import lsst.pex.policy as policy


# Constants
DC3PIPE_SVN_URL      = 'svn+ssh://svn.lsstcorp.org/DMS/ctrl/dc3pipe/trunk'
DC3PIPE_POLICY_DIR   = 'pipeline'

# If you change these not to use trunk, then please edit _retrieve()
ORCA_PIPECFG_SVN_URL = 'svn+ssh://svn.lsstcorp.org/DMS/ctrl/orca/trunk/policies/dc3pipe.paf'
ORCA_DBCFG_SVN_URL   = 'svn+ssh://svn.lsstcorp.org/DMS/ctrl/orca/trunk/policies/lsst10-mysql.paf'
ORCA_NODECFG_SVN_URL = 'svn+ssh://svn.lsstcorp.org/DMS/ctrl/orca/trunk/policies/lsstcluster.paf'

LOG_VIEW_URL_TMPL    = 'http://dev.lsstcorp.org/cgi-bin/showRun.cgi?%s'
LOG_DIR_TMPL         = '/lsst/DC3root/%s/work'
NODES                = ['lsst5:8', 'lsst6:8', 'lsst7:8', 
                        'lsst8:8', 'lsst9:8', 'lsst10:6']
# [(pipeline name in orca, pipeline policy dir name in dc3pipe), ]
PIPELINES          = {'imageSubtractionDetection': 'IPSD',
                      'mops':                      'nightmops'}
SVN_MODES          = ('co', 'checkout', 'export')


def run_dc3pipes(run_id, pipelines=[], nodes=[], master_policy=None,
                 setup_script=None, use_trunk=False, verbose=False):
    """
    Main work horse: set everything up and start a run.
    
    @param pipelines: list of pipelines to execute. If None, execute all.
    @param nodes: list of nodes to use. If None, use all.
    @param master_policy: use the master policy file master_policy instead of
           downloading a fresh copy from SVN.
    @param setup_script: use custom ORCA setup script.
    @param use_trunk: boolean - use policy files from trunk.
    @param verbose: verbosity flag. Default is False.
    
    @return 0 for success, otherwise error code.
    """
    try:
        setup_policy_files(pipelines, nodes, master_policy, use_trunk, verbose)
    except:
        sys.stderr.write('Error in setup_policy_files():\n')
        traceback.print_exc(file=sys.stdout)
        return(1)

    # Fire away!
    try:
        run(run_id, master_policy, setup_script, verbose)
    except:
        sys.stderr.write('Error in run():\n')
        traceback.print_exc(file=sys.stdout)
        return(2)
    return(0)


def run(run_id, master_policy=None, setup_script=None, verbose=False):
    """
    Everything should be ready: run the pipeline(s) as run run_id.
    """
    orca_dir = os.environ['CTRL_ORCA_DIR']
    orca_exe = os.path.join(orca_dir, 'bin', 'orca.py')
    
    if(not master_policy):
        pipeline_paf = os.path.basename(ORCA_PIPECFG_SVN_URL)
    else:
        pipeline_paf = os.path.basename(master_policy)
    
    if(setup_script):
        cmd = '%s -e %s %s %s' %(orca_exe, setup_script, pipeline_paf, run_id)
    else:
        cmd = '%s %s %s' %(orca_exe, pipeline_paf, run_id)

    if(verbose):
        print('-' * 80)
        print('*  Running "%s"' %(cmd))
        print('*  Have a nice day!')
        print('-' * 80)
    err = os.system(cmd)
    if(err):
        raise(Exception('Fatal error in "%s" (error code: %d)' %(cmd, err)))
    if(verbose):
        print('\n\n')
        print('It looks like everything wen well. Look at the logs at')
        print(LOG_VIEW_URL_TMPL %(run_id))
        print('Also, log files can be found in ' + LOG_DIR_TMPL %(run_id))
    return


def _valid_pipelines(pipelines):
    """
    We only support a few pipelines, defined in the PIPELINES list above.
    
    @param pipelines: list of pipeline names.
    
    @return True if pipelines is a subset of the supported pipeline list. False
            otherwise.
    """
    all_pipes = PIPELINES.keys()
    for p in pipelines:
        if(not p in all_pipes):
            return(False)
    return(True)


def _valid_nodes(nodes):
    """
    Just make sure that the machines in this list are specied in the 
    hostname:num_cores notation. Do not check and make sure that the machines 
    are pingable.
    
    @param nodes: list of nodes in hostname:num_cores MPI notation
    
    @return True is each node is in the desired notation. False otherwise.
    """
    for node in nodes:
        try:
            hostname, num_cores = node.split(':')
            num_cores = int(num_cores)
        except:
            return(False)
    return(True)
    

    
def setup_policy_files(pipelines, nodes, master_policy, 
                       use_trunk=False, verbose=False):
    """
    Checkout ctrl_dc3pipe and ctrl_orca policy files from SVN and, if pipelines
    and/or nodes and not empty, patch the files to reflect user's input.
    
    @param pipelines: list of pipelines to execute. If [], execute none.
    @param nodes: list of nodes to use. If [], use none.
    @param master_policy: use the master policy file master_policy instead of
           downloading a fresh copy from SVN.
    @param use_trunk: boolean - use policy files from trunk.
    @param verbose: verbosity flag. Default is False.
    
    @return None
    @throw Exception in case of error. The type of exception reflect the error.
    """    
    if(not pipelines or not nodes):
        raise(Exception('No pipelines to run and/or no nodes to use. Exiting.'))
    
    # Retrieve the top level policy file and patch it of needed.
    if(not master_policy):
        master_policy = os.path.abspath(os.path.basename(ORCA_PIPECFG_SVN_URL))
        _retrieve(ORCA_PIPECFG_SVN_URL, 'export', use_trunk, verbose)
    _patch_master_policy(master_policy, pipelines, verbose)
    
    # Get the node list and patch it if needed.
    _retrieve(ORCA_NODECFG_SVN_URL, 'export', use_trunk, verbose)
    _patch_node_policy(os.path.basename(ORCA_NODECFG_SVN_URL), nodes, verbose)
    
    # Get the DB config file.
    _retrieve(ORCA_DBCFG_SVN_URL, 'export', use_trunk, verbose)
    
    # Now we are ready to fetch the pipeline policy files.
    for pipe in PIPELINES.keys():
        root_name = PIPELINES[pipe]
        policy_url = os.path.join(DC3PIPE_SVN_URL, DC3PIPE_POLICY_DIR)
        
        # Export the main policy file.
        _retrieve(os.path.join(policy_url, '%s.paf' %(root_name)), 
                      'export', 
                      use_trunk,
                      verbose)
        
        # Checkout the stage policy files.
        _retrieve(os.path.join(policy_url, root_name), 'co', use_trunk, verbose)
    return


def _retrieve(url, mode='checkout', use_trunk=False, verbose=False):
    if(use_trunk):
        return(_svn_retrieve(url, mode, verbose))

    # In what follows we assume that the SVN URLs are using trunk. If you 
    # changed them to use tickets or tags and did not change what follows you
    # are evil! :-)
    # FIXME: this is a hack.
    if('orca/trunk/' in url):
        path = os.path.join(os.environ['CTRL_ORCA_DIR'], 
                            url.split('orca/trunk/')[-1])
    elif('dc3pipe/trunk/' in url):
        path = os.path.join(os.environ['CTRL_DC3PIPE_DIR'], 
                            url.split('dc3pipe/trunk/')[-1])
    else:
        raise(Exception('Fatal Error: I do not know what to do with %s' %(url)))
    
    if(mode == 'export'):
        print('cp %s %s' %(path, os.path.join('.', os.path.basename(path))))
        shutil.copyfile(path, os.path.join('.', os.path.basename(path)))
    else:
        if(path.endswith('/')):
            path = path[:-1]
        print('cp -r %s %s' %(path, os.path.join('.', os.path.basename(path))))
        shutil.copytree(path, os.path.join('.', os.path.basename(path)))
    return


def _svn_retrieve(url, mode='checkout', verbose=False):
    if(mode not in SVN_MODES):
        raise(Exception('Unknown SVN retrieve mode %s' %(mode)))
    
    cmd = 'svn %s %s' %(mode, url)
    if(verbose):
        print(cmd)
    err = os.system(cmd)
    if(err):
        raise(IOError('%s: no such file or directory' %(url)))
    return


def _patch_master_policy(file_name, pipelines, verbose=False):
    """
    Patch the master policy file (meaning the one that defines all the pipelines
    and where to find their policy files). In all cases, we have to edit 
    repositoryDirectory to point it to our local directory.
    
    @param file_name: path to the cluster policy file
    @param pipelines: list of pipeline names
    
    @return None
    """
    all_pipes = sets.Set(PIPELINES.keys())
    my_pipes = sets.Set(pipelines)
    
    # At a minimum, we need to patch repositoryDirectory to point to the local
    # directory.
    master_paf = policy.Policy(file_name)
    master_paf.set('repositoryDirectory', os.getcwd())
    
    # Let's see if we have to switch any pipeline off.
    if(my_pipes != all_pipes):
        all_pipes_paf = master_paf.get('pipelines')
        for pipe in all_pipes.difference(my_pipes):
            pipe_paf = all_pipes_paf.get(pipe)
            pipe_paf.set('launch', False)
    
    # Now we are ready to write the patched policy file back to disk.
    writer = policy.PAFWriter(file_name)
    writer.write(master_paf, True)
    writer.close()
    if(verbose):
        print('Patched %s' %(os.path.basename(file_name)))
    return


def _patch_node_policy(file_name, nodes, verbose=False):
    """
    Given a list of nodes (each of which in the hostname:num_cores MPI notation)
    and a cluster policy file name, modify the policy file to eflect the choice 
    of nodes.
    
    @param file_name: path to the cluster policy file
    @param nodes: list of nodes in hostname:num_cores MPI notation
    
    @return None
    """
    # Allright, let's patch this file.
    master_paf = policy.Policy(file_name)
    all_nodes_paf = master_paf.get('deploy')
    all_nodes = sets.Set(all_nodes_paf.getStringArray('nodes'))
    
    # Do we have anything to do?
    if(sets.Set(nodes) == all_nodes):
        return
    
    # Write the new 'nodes' array.
    all_nodes_paf.set('nodes', nodes[0])
    err = [all_nodes_paf.add('nodes', n) for n in nodes[1:]]
    
    # Now we are ready to write the patched policy file back to disk.
    writer = policy.PAFWriter(file_name)
    writer.write(master_paf, True)
    writer.close()
    if(verbose):
        print('Patched %s' %(os.path.basename(file_name)))
    return



if(__name__ == '__main__'):
    import optparse
    

    
    # Constants
    USAGE = """Usage
    run_dc3_pipelines.py OPTIONS <RUN_ID>

Cluster Options
    -n,--nodes      NODES: run on the space separated machine list NODES. 
                           machines are specified using the hostname:cores MPI
                           notation (e.g. localhost:8 for 8 cores on localhost)
                           default: run on all nodes.
    
Pipeline Options
    -p, --pipelines PIPES: execute the space separated pipeline list PIPES
                           default: execute all pipelines.
    -m,--master_paf  FILE: use FILE mas paster policy file (instead of 
                           dc3pipe.paf). FILE needs to be a valid top level ORCA
                           policy file.
                           default: use %s

General Options
    -s,--setup     SCRIPT: use SCRIPT to setup the pipeline execution 
                           environment. SCRIPT will be passd directly to ORCA.
                           default: use the standard ORCA script.
    -t,--trunk    Boolean: checkout policy files from the trunk instead of from
                           released packages. 
                           default: false
    -v,--verbose  Boolean: verbosity flag.
                           default: false


Example Usage

1. Execute mops on 4 cores on lsst9.ncsa.uiuc.edu only as run fra0001
   shell> run_dc3_pipelines.py -n lsst9.ncsa.uiuc.edu:4 -p mops fra0001

2. Execute imageSubtractionDetection on all nodes as run fra0002
   shell> run_dc3_pipelines.py -p imageSubtractionDetection fra0002

3. Execute all pipelines on all nodes as run fra0003
   shell> run_dc3_pipelines.py fra0003
"""
    # Get user input.
    parser = optparse.OptionParser(USAGE %(ORCA_PIPECFG_SVN_URL))
    parser.add_option('-n', '--nodes',
                      dest='nodes',
                      type='str',
                      default=None,
                      help='specify the machines to use.')
    parser.add_option('-p', '--pipelines',
                      dest='pipelines',
                      type='str',
                      default=None,
                      help='specify which pipelines to run.')
    parser.add_option('-s', '--setup',
                      dest='setup_script',
                      type='str',
                      default=None,
                      help='specify which setup script to use.')
    parser.add_option('-m', '--master_paf',
                      dest='master_policy',
                      type='str',
                      default=None,
                      help='specify which local master policy file to use.')
    parser.add_option('-t',
                      action='store_true',
                      dest='use_trunk',
                      default=False)
    # Verbose flag
    parser.add_option('-v',
                      action='store_true',
                      dest='verbose',
                      default=False)

    
    # Get the command line options and also whatever is passed on STDIN.
    (options, args) = parser.parse_args()

    # Make sure that we have a run_id.
    try:
        run_id = args[0]
    except:
        parser.error('Please specify a run ID (e.g. fra0001)')
    
    # Split pipelines and nodes into lists.
    all_pipes = PIPELINES.keys()
    if(options.pipelines):
        pipelines = options.pipelines.split()
        if(not _valid_pipelines(pipelines)):
            parser.error('I only know about these pipelines: %s' \
                         %(', '.join(all_pipes)))
    else:
        pipelines = PIPELINES.keys()
    # Handle nodes.
    if(options.nodes):
        nodes = options.nodes.split()
        if(not _valid_nodes(nodes)):
            parser.error('Each node in the node list has to be specified ' + \
                         'using the hostname:cores MPI notation.\n '+ \
                         '(e.g. localhost:8 for 8 cores on localhost)')
    else:
        nodes = NODES
    # Handle setup_script.
    if(options.setup_script):
        setup_script = os.path.abspath(options.setup_script)
    else:
        setup_script = None
    # Handle master policy file override.
    if(options.master_policy):
        master_policy = os.path.abspath(options.master_policy)
    else:
        master_policy = None
    
    # If we do not use trunk, we have to make sure that orca and dc3pipe are 
    # setup.
    if(not options.use_trunk and 'CTRL_ORCA_DIR' not in os.environ.keys()):
        sys.stderr.write('Please setup ctrl_orca and re-run this script.\n')
        sys.exit(1)
    if(not options.use_trunk and 'CTRL_DC3PIPE_DIR' not in os.environ.keys()):
        sys.stderr.write('Please setup ctrl_dc3pipe and re-run this script.\n')
        sys.exit(2)
    
    # Everything is fine: let's rock!
    sys.exit(run_dc3pipes(run_id, pipelines, nodes, master_policy,
                          setup_script, options.use_trunk, options.verbose))

