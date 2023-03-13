import { AwsCredentialIdentity, Provider } from '@aws-sdk/types';
import { fromIni } from '@aws-sdk/credential-providers';

export function getCredential(credentials: any ):  AwsCredentialIdentity | Provider<AwsCredentialIdentity> {
  if (credentials.profile) {
    const credential = fromIni({ profile: credentials.profile });
    return credential;
  }

  if (!(credentials.accessKeyId && credentials.secretAccessKey)) {
    throw new Error('AWS profile name or ID - Secret access key pair, please provide one.');
  }

  return <AwsCredentialIdentity>{
    accessKeyId: credentials.accessKeyId,
    secretAccessKey: credentials.secretAccessKey,
    sessionToken: credentials.sessionToken,
  };
}