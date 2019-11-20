#!/bin/bash

if [ -z "$1" ]; then
  echo "Please specify the version to be used for new release."
  exit 1
fi
echo "Releasing version $1 for project."

cd $(dirname "$0")

echo "Excuting all tests."
TEST_FAIL=$(vendor/bin/phpunit test/AllTests.php | tail -n 2 | head -n 1 | grep -vc 'OK')
if [ "$TEST_FAIL" != '0' ]; then
  echo "Test didn't pass so we can't continue"
  exit 1
fi
echo "All Tests passed we can continue to deploy and updating to version $1."
php -r "\$f=json_decode(file_get_contents('composer.json'), true);\$f['version']='$1';file_put_contents('composer.json', preg_replace('/  /', ' ', json_encode(\$f, JSON_PRETTY_PRINT | JSON_UNESCAPED_SLASHES)));"
composer update
git add composer.json composer.lock
git commit -m "Release new version $1"
git tag "$1"
echo "Push new tag to gitlab."
git push
echo "Push new tag to nexus repository."
composer nexus-push $1 \
  --url https://ovh-repo-01.v3d.fr/repository/v3d-php \
  --ignore-dirs share test \
  --ignore sonar-project.properties Jenkinsfile release.sh \
  --src-url "$(git remote get-url origin)" \
  --src-ref "$(git rev-parse HEAD)" \
  "$1"

echo "All done."
exit 0
