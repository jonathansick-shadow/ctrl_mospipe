# -*- python -*-
#
# Setup our environment
#
import glob, os.path, re, os
import lsst.SConsUtils as scons

env = scons.makeEnv("ctrl_mospipe", r"$HeadURL: svn+ssh://taxelrod@lsstarchive.ncsa.uiuc.edu/DMS/ctrl/mospipe/trunk/SConstruct $")



env['IgnoreFiles'] = r"(~$|\.pyc$|^\.svn$|\.o$)"

# the "install" target
#
Alias("install", [env.Install(env['prefix'], "bin"),
                  env.Install(env['prefix'], "etc"),
                  env.Install(env['prefix'], "pipeline"),
                  env.Install(env['prefix'], "python"),
                  env.InstallEups(env['prefix'] + "/ups",
                                  glob.glob("ups/*.table"))])

scons.CleanTree(r"*~ core *.os *.o")

#
# Build TAGS files
#
files = scons.filesToTag()
if files:
    env.Command("TAGS", files, "etags -o $TARGET $SOURCES")

env.Declare()
env.Help("""
LSST package for executing DC3
""")
