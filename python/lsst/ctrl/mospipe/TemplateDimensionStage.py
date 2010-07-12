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
from lsst.pex.harness import Utils
from lsst.daf.persistence import LogicalLocation
import lsst.afw.image as afwImage

class TemplateDimensionStage(Stage):
    def process(self):
        clipboard = self.inputQueue.getNextDataset()

        additionalData = Utils.createAdditionalData(self,
                self._policy, clipboard)
        templateLocation = self._policy.get('templateLocation')
        templatePath = LogicalLocation(templateLocation,
                additionalData).locString()
        metadata = afwImage.readMetadata(templatePath)
        dims = afwImage.PointI(metadata.get("NAXIS1"), metadata.get("NAXIS2"))
        outputKey = self._policy.get('outputKey')
        clipboard.put(outputKey, dims)

        self.outputQueue.addDataset(clipboard)
