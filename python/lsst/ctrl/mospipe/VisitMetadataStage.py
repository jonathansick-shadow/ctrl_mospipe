from lsst.pex.harness.Stage import Stage
from lsst.daf.persistence import LogicalLocation
from lsst.daf.base import PropertySet, DateTime
import lsst.afw.image as afwImage

class VisitMetadataStage(Stage):
    def preprocess(self):
        self.activeClipboard = self.inputQueue.getNextDataset()

        eventName = self._policy.get("inputEvent")
        event = self.activeClipboard.get(eventName)
        visitId = event.get("visitId")
        exposureId = event.get("exposureId")

        fpaExposureId = (long(visitId) << 1) + exposureId

        visit = PropertySet()
        visit.setInt("visitId", visitId)
        visit.setLongLong("exposureId", fpaExposureId)
        self.activeClipboard.put("visit" + str(exposureId), visit)

        rawFpaExposure = PropertySet()
        rawFpaExposure.setLongLong("rawFPAExposureId", fpaExposureId)
        rawFpaExposure.set("ra", event.get("ra"))
        rawFpaExposure.set("decl", event.get("decl"))
        rawFpaExposure.set("filterId",
                self.lookupFilterId(event.get("filter")))
        rawFpaExposure.set("equinox", event.get("equinox"))
        rawFpaExposure.set("dateObs", DateTime(event.get("dateObs")))
        rawFpaExposure.set("mjdObs", DateTime(event.get("dateObs")).mjd())
        rawFpaExposure.set("expTime", event.get("expTime"))
        rawFpaExposure.set("airmass", event.get("airmass"))
        self.activeClipboard.put("fpaExposure" + str(exposureId), rawFpaExposure)

        # rely on default postprocess() to move self.activeClipboard to output queue

    def process(self):
        clipboard = self.inputQueue.getNextDataset()

        eventName = self._policy.get("inputEvent")
        event = clipboard.get(eventName)
        visitId = event.get("visitId")
        exposureId = event.get("exposureId")

        ccdId = clipboard.get("ccdId")
        ampId = clipboard.get("ampId")

        fpaExposureId = (long(visitId) << 1) + exposureId
        ccdExposureId = (fpaExposureId << 8) + ccdId
        ampExposureId = (ccdExposureId << 6) + ampId

        clipboard.put("visitId", visitId)

        exposureMetadata = PropertySet()
        exposureMetadata.setInt("filterId",
                self.lookupFilterId(event.get("filter")))
        exposureMetadata.setLongLong("fpaExposureId", fpaExposureId)
        exposureMetadata.setLongLong("ccdExposureId", ccdExposureId)
        exposureMetadata.setLongLong("ampExposureId", ampExposureId)
        clipboard.put("exposureMetadata" + str(exposureId), exposureMetadata)

        self.outputQueue.addDataset(clipboard)

    def lookupFilterId(self, filterName):
        dbLocation = LogicalLocation("%(dbUrl)")
        filterDb = afwImage.Filter(dbLocation, filterName)
        filterId = filterDb.getId()
        return filterId
