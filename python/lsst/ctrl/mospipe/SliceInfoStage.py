#! /usr/bin/env python

from lsst.pex.harness.Stage import Stage
from lsst.pex.policy import Policy
import lsst.afw.image as afwImage

class SliceInfoStage(Stage):
    '''Compute per-slice information.'''

    def __init__(self, stageId=-1, stagePolicy=None):
        Stage.__init__(self, stageId, stagePolicy)
        self.ampBBoxDb = Policy(self._policy.get("ampBBoxDbPath"))

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

        ccdId = eval(ccdFormula)
        ampId = eval(ampFormula)

        clipboard.put("ccdId", ccdId)
        clipboard.put("ampId", ampId)
        clipboard.put("ampBBox", self.lookupAmpBBox(ampId, ccdId))

    def lookupAmpBBox(self, ampId, ccdId):
        key = "CcdBBox.Amp%d" % ampId
        p = self.ampBBoxDb.get(key)
        return afwImage.BBox(
                afwImage.PointI(p.get("x0"), p.get("y0")),
                p.get("width"), p.get("height"))
