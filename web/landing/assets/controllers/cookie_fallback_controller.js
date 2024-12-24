import { Controller } from "@hotwired/stimulus";

/* stimulusFetch: 'lazy' */
export default class extends Controller {

    connect() {}

    accept() {
        localStorage.setItem("cookie-consent", "accepted");

        this.consentGranted();

        window.location = '/';
    }

    consentGranted() {
        gtag("consent", "update", {
            ad_user_data: 'granted',
            ad_personalization: 'granted',
            ad_storage: 'granted',
            analytics_storage: 'granted'
        });
    }
}
