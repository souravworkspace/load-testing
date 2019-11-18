import dotenv from 'dotenv';

dotenv.config();

const ENV = process.env;

function must(envName: string): string {
  if (ENV[envName]) return <string>ENV[envName];
  else throw new Error(`environment.variable.missing ${envName}`);
}

export default {
  isDev: ENV.NODE_ENV === 'development',
  servicePG: ENV.SERVICE_PG || 'postgresql://ccart:@postgres:5432/tracker',
}
