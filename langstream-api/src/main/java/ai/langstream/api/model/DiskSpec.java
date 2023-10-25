/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package ai.langstream.api.model;

import lombok.extern.slf4j.Slf4j;

/** Definition of the disk resources required by the agent. */
@Slf4j
public record DiskSpec(Boolean enabled, String type, String size) {

    public static final String TYPE_DEFAULT = "default";
    public static final String SIZE_DEFAULT = "256M";

    public DiskSpec {
        if (enabled != null && enabled) {
            if (type == null || type.isBlank()) {
                type = TYPE_DEFAULT;
            }
            if (size == null) {
                size = SIZE_DEFAULT;
            }
        }
        parseSize(size);
    }

    public static long parseSize(String diskSize) {
        if (diskSize == null || diskSize.isEmpty()) {
            return 0;
        }

        long multiplier = 1;
        diskSize =
                diskSize.toUpperCase()
                        .trim() // Convert the input to uppercase for case-insensitivity.
                        .replaceAll("\\s+", ""); // remove spaces

        if (diskSize.endsWith("G")) {
            multiplier = 1024L * 1024L * 1024L;
            diskSize = diskSize.substring(0, diskSize.length() - 1);
        } else if (diskSize.endsWith("M")) {
            multiplier = 1024L * 1024L;
            diskSize = diskSize.substring(0, diskSize.length() - 1);
        } else if (diskSize.endsWith("K")) {
            multiplier = 1024L;
            diskSize = diskSize.substring(0, diskSize.length() - 1);
        }

        try {
            long size = Long.parseLong(diskSize);
            log.info(
                    "Parsed disk size {} as {} bytes (multiplier is {})",
                    diskSize,
                    size * multiplier,
                    multiplier);
            return size * multiplier;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException(
                    "Invalid disk size string " + diskSize + ": " + e.getMessage());
        }
    }

    public static DiskSpec DEFAULT = new DiskSpec(false, TYPE_DEFAULT, SIZE_DEFAULT);

    public DiskSpec withDefaultsFrom(DiskSpec higherLevel) {
        if (higherLevel == null) {
            return this;
        }
        Boolean newEnabled = enabled == null ? higherLevel.enabled() : enabled;
        String newType = type == null ? higherLevel.type() : type;
        String newSize = size == null ? higherLevel.size() : size;
        return new DiskSpec(newEnabled, newType, newSize);
    }
}
