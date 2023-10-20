# LangStream Chatbot UI

Try out your LangStream application with this React app.

## Install

```
npm install
```

## Configure

Export your env variables to configure the UI to connect to your application.

```
export REACT_APP_WEBSOCKET_URL=...
export REACT_APP_APP=...
export REACT_APP_TENANT=...
export REACT_APP_CONSUMER=...
export REACT_APP_PRODUCER=...
export REACT_APP_CREDENTIALS=..
```
or add your app details to `config.ts`

You can use a helper script to get the environment variable settings from the langstream CLI output. To get some of the env vars from the current profile, use this command:

```
langstream profiles get `langstream profiles get-current` -o yaml | ./get-env-vars-from-profile.sh
```

You can copy and paste the results into your terminal.

To get some of the env vars from the application, use this command:

```
langstream apps get <app-name> -o yaml | ./get-env-vars-from-app.sh
```

Again, copy and paste the results in your terminal.

These scripts will give you commands for all environment variables except `REACT_APP_CREDENTIALS`. You will have to set that manually.

## Run

```
npm run start
```

To run the UI at [http://localhost:4000](http://localhost:4000)



This project was bootstrapped with [Create React App](https://github.com/facebook/create-react-app).

## Available Scripts

In the project directory, you can run:

### `npm start`

Runs the app in the development mode.\
Open [http://localhost:4000](http://localhost:4000) to view it in the browser.

The page will reload if you make edits.\
You will also see any lint errors in the console.

### `npm test`

Launches the test runner in the interactive watch mode.\
See the section about [running tests](https://facebook.github.io/create-react-app/docs/running-tests) for more information.

### `npm run build`

Builds the app for production to the `build` folder.\
It correctly bundles React in production mode and optimizes the build for the best performance.

The build is minified and the filenames include the hashes.\
Your app is ready to be deployed!

See the section about [deployment](https://facebook.github.io/create-react-app/docs/deployment) for more information.

### `npm run eject`

**Note: this is a one-way operation. Once you `eject`, you can’t go back!**

If you aren’t satisfied with the build tool and configuration choices, you can `eject` at any time. This command will remove the single build dependency from your project.

Instead, it will copy all the configuration files and the transitive dependencies (webpack, Babel, ESLint, etc) right into your project so you have full control over them. All of the commands except `eject` will still work, but they will point to the copied scripts so you can tweak them. At this point you’re on your own.

You don’t have to ever use `eject`. The curated feature set is suitable for small and middle deployments, and you shouldn’t feel obligated to use this feature. However we understand that this tool wouldn’t be useful if you couldn’t customize it when you are ready for it.

## Learn More

You can learn more in the [Create React App documentation](https://facebook.github.io/create-react-app/docs/getting-started).

To learn React, check out the [React documentation](https://reactjs.org/).
