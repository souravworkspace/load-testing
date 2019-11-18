import _ from 'lodash';
import P from 'bluebird';
import generate from 'nanoid/generate';
import got from 'got';

let runCount = 3;

interface IUserEvent {
  userId: string,
  domainId: string,
  eventId: string,
  pageId: string,
  tabId: string,
  event: string,
  genAt: number,
}

function genDomainId() {
  return `${generate('0123456789', 6)}`;
}

function genUserId() {
  return `${generate('0123456789', 5)}.${generate('0123456789', 5)}`;
}

function randId(num: number = 6) {
  return generate('0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', num);
}

function  getUserEvents(data: { domainId: string, userIds: string[] }): IUserEvent[] {
  const { domainId, userIds } = data;
  return _(userIds)
    .map(userId => Array.from(Array(60).keys()).map(i => {
      const pageId = randId(4);
      const tabId = randId(4);
      return [{
        userId,
        domainId,
        eventId: randId(8),
        pageId,
        tabId,
        event: 'pageViewed',
        genAt: -100,
      }, {
        userId,
        domainId,
        eventId: randId(8),
        pageId,
        tabId,
        event: 'homapageViewed',
        genAt: -100,
      }];
    }))
    .flatten()
    .flatten()
    .value();
}

async function uploadEvent(event:  {domainId: string, userId: string }) {
  const delay = Math.round(Math.random() * 10) * Math.round(Math.random() * 10);
  await P.delay(delay);
  const { domainId, userId, ...restEv } = event
  try {
    await got(`http://localhost:3060/event/v3/${domainId}/${userId}`, {
      method: 'POST',
      json: true,
      body: [restEv],
      timeout: 5000
    });
  } catch(err) {
    console.error('got.timeout');
  }
}

async function uploadChunk(uEvents: IUserEvent[]) {
  const start = Date.now();
  await P.map(uEvents, uploadEvent, {concurrency: 100});
  const processTime = Math.round(Date.now() - start);
  console.log('processTime: ', processTime);
  if(processTime < 1000) {
    await P.delay(1000 - processTime);
  }
}

async function saveParallel() {
  const start = Date.now();
  const domainIds = (Array.from(Array(10).keys())).map(r => genDomainId());
  const eventsChunk = _(domainIds)
    .map(domainId => ({ domainId, userIds: Array.from(Array(100).keys()).map(u => genUserId())}))
    .map(data => getUserEvents(data))
    .flatten()
    .chunk(22000)
    .value();
  
  await P.map(eventsChunk, uploadChunk, { concurrency: 1 });
  const processTime = Math.round((Date.now() - start)/1000);
  console.log(`Processed in: ${processTime} Sec`);
}

async function run() {
  await saveParallel();
  await P.delay(300);
  runCount--;
  if (runCount > 0) await run();
}

async function main() {
  const stopServer = () => {
    console.log('shutting.down');
    runCount = 0;
    process.exit(1);
  };
  process.once('SIGINT', stopServer);
  process.once('SIGTERM', stopServer);
  run();
}

main()
  .catch(err => {
    console.error('pg.fail', err);
  });
