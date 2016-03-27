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
