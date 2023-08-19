# Release process for LangStream

## Overview

1. [ Release the project and trigger the CI pipeline.](#release-the-current-code-and-crds)
2. [Update the HomeBrew formula with the new version.](#update-the-homebrew-formula)
3. [Update the Helm chart with the new version, if needed.](#update-the-helm-chart)


## Release the current code and CRDs

```
mvn release:prepare
```

After this command:
- The new docker images will be available
- A new GitHub release will be created

## Update the HomeBrew formula

Update the formulae with the new version tarball url:
1. Open the file [`langstream.rb`](https://github.com/LangStream/homebrew-langstream/blob/main/langstream.rb)
2. Insert the new version
3. Update the sha256 checksum (`sha256sum langstream-<version>.zip`)

## Update the Helm chart

This step is optional and recommended only for new stable versions. 
1. Set the new version in the [`values.yaml`](https://github.com/LangStream/charts/blob/main/charts/langstream/values.yaml) file.
2. Update the CRDs running the following command:
```
git clone https://github.com/LangStream/charts langstream-charts
cd langstream-charts
./import-langstream-crds.sh <version>
git commit -am "Import new CRDs" && git push origin
```
CRDs normally doesn't change so this step is not needed for every release but if they do, you must update them the Helm chart.
3. Cut a release chart release. Update the version in the [Chart.yaml](https://github.com/LangStream/charts/blob/main/charts/langstream/Chart.yaml) file and push the change. 
