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
from lsst.daf.persistence import LogicalLocation, DbStorage


class CcdMetadataStage(Stage):

    def preprocess(self):
        self.activeClipboard = self.inputQueue.getNextDataset()
        fpaExposureId0 = self.activeClipboard.get('visit0').get('exposureId')
        fpaExposureId1 = self.activeClipboard.get('visit1').get('exposureId')

        db = DbStorage()
        loc = LogicalLocation("%(dbUrl)")
        db.setPersistLocation(loc)
        db.startTransaction()
        db.executeSql("""
            INSERT INTO Raw_CCD_Exposure
            SELECT DISTINCT rawCCDExposureId, rawFPAExposureId
            FROM Raw_Amp_Exposure
            WHERE rawFPAExposureId = %d OR rawFPAExposureId = %d
        """ % (fpaExposureId0, fpaExposureId1) )
        db.executeSql("""
            INSERT INTO Science_CCD_Exposure
            SELECT DISTINCT
                scienceCCDExposureId, scienceFPAExposureId,
                scienceCCDExposureId
            FROM Science_Amp_Exposure
            WHERE scienceFPAExposureId = %d OR scienceFPAExposureId = %d
        """ % (fpaExposureId0, fpaExposureId1) )
        db.executeSql("""
            INSERT INTO Science_FPA_Exposure
            SELECT DISTINCT scienceFPAExposureId
            FROM Science_Amp_Exposure
            WHERE scienceFPAExposureId = %d OR scienceFPAExposureId = %d
        """ % (fpaExposureId0, fpaExposureId1) )
        db.endTransaction()

        # rely on default postprocess() to move self.activeClipboard to output queue
