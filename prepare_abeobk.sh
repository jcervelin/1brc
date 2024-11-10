#!/bin/bash
source "$HOME/.sdkman/bin/sdkman-init.sh"
sdk use java 21.0.2-graal 1>&2

# ./mvnw clean verify removes target/ and will re-trigger native image creation.
if [ ! -f target/CalculateAverage_abeobk_image ]; then
    NATIVE_IMAGE_OPTS="--gc=epsilon -O3 -march=native -H:InlineAllBonus=10 -H:-GenLoopSafepoints --enable-preview --initialize-at-build-time=io.jcervelin.CalculateAverage"
    native-image $NATIVE_IMAGE_OPTS -cp target/average-1.0.0-SNAPSHOT.jar -o target/CalculateAverage_abeobk_image dev.morling.onebrc.CalculateAverage_abeobk
fi
