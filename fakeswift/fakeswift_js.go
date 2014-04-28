package fakeswift

const FakeSwiftJs = `
var LightSwift = require('light-swift');

var swift = new LightSwift({
  port: %d
});

swift.addAccount('test');
swift.addUser('test', 'tester', 'testing');
swift.addContainer('test', 'test-container');

swift.importObjects('test', 'test-container', [{
  name: 'file.txt',
  contentType: 'text/plain',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: '827ccb0eea8a706c4c34a16891f84e7b',
  content: '12345'
}, {
  name: 'dir1/file1.txt',
  contentType: 'text/plain',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: 'a09ebcef8ab11daef0e33e4394ea775f',
  content: 'dir1/file1'
}, {
  name: 'dir1/file2.txt',
  contentType: 'text/plain',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: '725d747aeea47a4d8f6432ef5a9ee268',
  content: 'dir1/file2'
}, {
  name: 'dir1/dir2/file3.txt',
  contentType: 'text/plain',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: '76413a4ca3f356a90658059282f905bf',
  content: 'dir1/dir2/file3'
}, {
  name: 'dir1/dir3',
  contentType: 'application/directory',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: 'd41d8cd98f00b204e9800998ecf8427e',
  content: ''
}, {
  name: 'dir1/dir4',
  contentType: 'text/directory',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: 'd41d8cd98f00b204e9800998ecf8427e',
  content: ''
}, {
  name: 'big-file.txt',
  contentType: 'application/octet-stream',
  lastModified: new Date('2013-04-22T16:58:36.698Z'),
  hash: '3838b518af215da4f6042b1679efdd44',
  contentLength: (1 * 1024 * 1024) - 1,
  content: (new Array(1 * 1024 * 1024)).join('x')
}])

swift.server();

console.log('RUNNING');
`
