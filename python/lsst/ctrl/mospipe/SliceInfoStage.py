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


from lsst.pex.harness.Stage import Stage
from lsst.pex.policy import Policy
import lsst.afw.image as afwImage

class SliceInfoStage(Stage):
    '''Compute per-slice information.'''

    def __init__(self, stageId=-1, stagePolicy=None):
        Stage.__init__(self, stageId, stagePolicy)

    def preprocess(self): 
        self.activeClipboard = self.inputQueue.getNextDataset()
        self._impl(self.activeClipboard)
        # Let postprocess() put self.activeClipboard on the output queue

    def process(self): 
        """
        Compute the ampId and ccdId corresponding to this slice.
        """
        clipboard = self.inputQueue.getNextDataset()
        self._impl(clipboard)
        self.outputQueue.addDataset(clipboard)

    def _impl(self, clipboard):
        sliceId = self.getRank()

        nAmps = self._policy.get("nAmps")
        nCcds = self._policy.get("nCcds")

        ccdFormula = self._policy.get("ccdIdFormula")
        ampFormula = self._policy.get("ampIdFormula")
        hduFormula = self._policy.get("hduIdFormula")


        ccdId = eval(ccdFormula)
        ampId = eval(ampFormula)
        hduId = eval(hduFormula)

        clipboard.put("ccdId", ccdId)
        clipboard.put("ampId", ampId)
        clipboard.put("hduId", hduId)
