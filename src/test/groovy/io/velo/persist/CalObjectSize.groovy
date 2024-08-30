package io.velo.persist


import org.openjdk.jol.info.ClassLayout

println ClassLayout.parseClass(LocalPersist.SegmentCacheKey).toPrintable()