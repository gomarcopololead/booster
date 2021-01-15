# Authentication Booster Rocket for AWS

This package is a configurable Booster rocket to add an authentication api based on Cognito to your Booster applications.

## Usage

Install this package as a dev dependency in your Booster project (It's a dev dependency because it's only used during deployment, but we don't want this code to be uploaded to the project lambdas)

```sh
yarn add --dev @boostercloud/rocket-auth-aws-infrastructure
```

In your Booster config file, pass a `RocketDescriptor` array to the `AWS Provider` initializer configuring the aws authentication rocket:

```typescript
import { Booster } from '@boostercloud/framework-core'
import { BoosterConfig } from '@boostercloud/framework-types'
import * as AWS from '@boostercloud/framework-provider-aws'


Booster.configure('production', (config: BoosterConfig): void => {
  config.appName = 'my-store'
  config.provider = Provider([
    {
      packageName: '@boostercloud/rocket-auth-aws-infrastructure',
      parameters: {
        appName: config.appName,                  
        environmentName: config.environmentName,
        mode: 'Passwordless',                     
      },
    },
  ])
})
```

## Configuration Options

```typescript
{
  appName: string                            // Required
  environmentName: string                    // Required
  passwordPolicy?: {                         // Optional, all values are set to true by default.
    minLength?: number                       // Minimum length, which must be at least 6 characters but fewer than 99 character
    requireDigits: boolean                   // Require numbers
    requireLowercase: boolean                // Require lowercase letters
    requireSymbols: boolean                  // Require a special characters
    requireUppercase: boolean                // Require uppercase letters
  }
  mode: 'Passwordless' | 'UserPassword'      // One time password the user must be a phone number | User Password mode, the user must be a valid email.
}
```

## Outputs

The auth rocket will expose the following base url outputs:

```sh
<appName>.AuthApiEndpoint = https://<httpURL>/production/auth
<appName>.AuthApiIssuer = https://cognito-idp.<app-region>.amazonaws.com/{userPoolId}
<appName>.AuthApiJwksUri = https://cognito-idp.<app-region>.amazonaws.com/{userPoolId}/.well-known/jwks.json
```

| Output             | Description                                                                            |
| ------------------ | -------------------------------------------------------------------------------------- |
| AuthApiEndpoint    | Base Api Url which will exposed all auth endpoints.                                     |
| AuthApiIssuer      | The issuer who sign the JWT tokens.                                                     |
| AuthApiJwksUri     | Uri with all the public keys used to sign the JWT tokens.                               |







The `AuthApiIssuer` and `AuthApiJwksUri` must be used in the `tokenVerifier` Booster config. More information about JWT Configuration [here.](https://github.com/boostercloud/booster/blob/master/docs/README.md#jwt-configuration)

### Sign-up

Users can use this endpoint to register in your application and get a role assigned to them.

#### Endpoint

```http request
POST https://<httpURL>/auth/sign-up
```

#### Request body

```json
{
  "username": "string",
  "password": "string",
  "userAttributes": {
    "role": "string",
  }
}
```

| Parameter        | Description                                                                                                                                                                                          |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| _username_       | The username of the user you want to register. It **must be an email in UserPassword mode or an phone number  in Passwordless mode**.                                                                                                                              |
| _password_       | The password the user will use to later login into your application and get access tokens. **Only in UserPassword mode**                                                                                                           |
| _userAttributes_ | Here you can specify the attributes of your user. These are: <br/> -_**role**_: A unique role this user will have. You can only specify here a role where the `signUpOptions` property is not empty.|

#### Response

```json
{
  "id": "cb61c0a4-8f85-4774-88a9-448ce6321eea",
  "username": "+34999999999",
  "userAttributes": {
    "role": "User"
  }
}
```

#### Errors

You will get an HTTP status code different from 2XX and a body with a message telling you the reason of the error.

Example of an account with the given username which already exists:

```json
{
  "error": {
    "type": "UsernameExistsException",
    "message": "An account with the given phone_number already exists."
  }
}
```

You will get an HTTP status code different from 2XX and a body with a message telling you the reason of the error.

### Confirm Sign Up

Whenever a User signs up with their phone number in `Passwordless` mode, an SMS message will be sent with a confirmation code. If you're using a `UserPassword` mode an email with a confirmation link will be sent.
They will need to provide this code to confirm registation by calling the`sign-up/confirm` endpoint

#### Endpoint

```http request
POST https://<httpURL>/auth/sign-up/confirm
```

#### Request body

```json
{
  "confirmationCode": "string",
  "username": "string"
}
```

| Parameter          | Description                                                                            |
| ------------------ | -------------------------------------------------------------------------------------- |
| _confirmationCode_ | The confirmation code received in the SMS message.                                     |
| _username_         | The username of the user you want to sign in. They must have previously signed up.     |

#### Response

```json
{
  "message": "The username: +34999999999 has been confirmed."
}

#### Errors

You will get an HTTP status code different from 2XX and a body with a message telling you the reason of the error.
Common errors would be like submitting an expired confirmation code or a non valid one.

Example of an invalid verification code:

```json
{
  "error": {
    "type": "CodeMismatchException",
    "message": "Invalid verification code provided, please try again."
  }
}
```

### Resend Sing Up Confirmation Code

If for whatever reason the confirmationCode never reach your email or phone you can ask the api to resend a new one.

#### Endpoint

```http request
POST https://<httpURL>/auth/sign-up/resend-code
```

#### Request body

```json
{
  "username": "string"
}
```

| Parameter          | Description                                                                            |
| ------------------ | -------------------------------------------------------------------------------------- |
| _username_         | The username of the user you want to sign in. They must have previously signed up.     |

#### Response

```json
{
  "message": "The confirmation code to activate your account has been sent to: +34999999999."
}

#### Errors

You will get an HTTP status code different from 2XX and a body with a message telling you the reason of the error.
Common errors would be like submitting an expired confirmation code or a non valid one.


### Sign-in

This endpoint creates a session for an already registered user, returning an access token that can be used to access role-protected resources

###### Endpoint

```http request
POST https://<httpURL>/auth/sign-in
```

###### Request body

```json
{
  "username": "string",
  "password": "string"
}
```

| Parameter  | Description                                                                            |
| ---------- | -------------------------------------------------------------------------------------- |
| _username_ | The username of the user you want to sign in. They must have previously signed up.     |
| _password_ | The password used to sign up the user.                                                 |

#### Response UserPassword mode

```json
{
  "accessToken": "string",
  "expiresIn": "string",
  "refreshToken": "string",
  "tokenType": "string"
}
```

#### Response PasswordLess mode

```json
{
  "accessToken": "string",
  "expiresIn": "string",
  "refreshToken": "string",
  "tokenType": "string" 
}

| Parameter      | Description                                                                                                                          |
| -------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| _accessToken_  | The token you can use to access restricted resources. It must be sent in the `Authorization` header (prefixed with the `tokenType`). |
| _expiresIn_    | The period of time, in seconds, after which the token will expire.                                                                   |
| _refreshToken_ | The token you can use to get a new access token after it has expired.                                                                |
| _tokenType_    | The type of token used. It is always `Bearer`.                                                                                       |

###### Errors

You will get an HTTP status code different from 2XX and a body with a message telling you the reason of the error.

Example: Login of a user that has not been confirmed

```json
{
  "__type": "UserNotConfirmedException",
  "message": "User is not confirmed."
}
```

##### Sign-out

Users can call this endpoint to finish the session.

###### Endpoint

```http request
POST https://<httpURL>/auth/sign-out
```

###### Request body

```json
{
  "accessToken": "string"
}
```

| Parameter     | Description                                   |
| ------------- | --------------------------------------------- |
| _accessToken_ | The access token you get in the sign-in call. |

###### Response

An empty body

###### Errors

You will get an HTTP status code different from 2XX and a body with a message telling you the reason of the error.

Example: Invalid access token specified

```json
{
  "__type": "NotAuthorizedException",
  "message": "Invalid Access Token"
}
```

##### Refresh token

Users can call this endpoint to refresh the access token.

###### Endpoint

```http request
POST https://<httpURL>/auth/refresh-token
```

###### Request body

> Refresh-token request body

```json
{
  "clientId": "string",
  "refreshToken": "string"
}
```

| Parameter      | Description                                                                            |
| -------------- | -------------------------------------------------------------------------------------- |
| _clientId_     | The application client Id that you got as an output when the application was deployed. |
| _refreshToken_ | The token you can use to get a new access token after it has expired.                  |

###### Response

```json
{
  "accessToken": "string",
  "expiresIn": "string",
  "refreshToken": "string",
  "tokenType": "string"
}
```

###### Errors

> Refresh token error response body example: Invalid refresh token specified

```json
{
  "__type": "NotAuthorizedException",
  "message": "Invalid Refresh Token"
}
```

You will get a HTTP status code different from 2XX and a body with a message telling you the reason of the error.