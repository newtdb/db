========
Road map
========

- More documentation

- Asynchronous updates

  IF you have a lot of indexes to update, you'll be able to update
  them asynchronously. This can be important if primary writes need to
  happen quickly.

  The asynchronous updater will also make it easier to try out newt
  with and migrate existing RelStorage applications.

- Custom JSON generation

  There will be an API to customize how JSON data are generated. This
  can be especially helpful when using data that's not easy to
  manipulate in Postgresql.
