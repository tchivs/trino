/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.paimon;

import java.io.File;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

final class PaimonWriteSpillPaths
{
    private static final String PATH_SEPARATOR_REGEX = ",|" + Pattern.quote(File.pathSeparator);

    private PaimonWriteSpillPaths() {}

    static String[] split(String writeSpillPath)
    {
        requireNonNull(writeSpillPath, "writeSpillPath is null");
        String[] tempDirs = writeSpillPath.length() > 0 ? writeSpillPath.split(PATH_SEPARATOR_REGEX, -1) : new String[0];
        checkArgument(tempDirs.length > 0, "write.spill-path must contain at least one path");
        checkArgument(Stream.of(tempDirs).noneMatch(String::isBlank),
                "write.spill-path must not contain empty path entries");
        return tempDirs;
    }

    static boolean hasValidEntries(String writeSpillPath)
    {
        if (writeSpillPath == null || writeSpillPath.isBlank()) {
            return true;
        }
        return Stream.of(writeSpillPath.split(PATH_SEPARATOR_REGEX, -1))
                .noneMatch(String::isBlank);
    }
}
