package com.ovoenergy.comms.deduplication

import com.ovoenergy.comms.deduplication.meteor.model._

package object meteor {
  type MeteorDeduplication[F[_], ID, ContextID] = Deduplication[F, ID, ContextID, EncodedResult]
}
