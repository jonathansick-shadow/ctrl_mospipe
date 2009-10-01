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
