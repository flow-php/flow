import { Controller } from "@hotwired/stimulus";

/* stimulusFetch: 'lazy' */
export default class extends Controller {
    static targets = ["banner", "button"];
    static values = {
        gaId: String
    }

    connect() {
        const consentStatus = localStorage.getItem("cookie-consent");
        if (consentStatus) {
            this.bannerTarget.classList.add("hidden");
            // If accepted previously, inject GA
            if (consentStatus === "accepted") {
                this.consentGranted();
            } else {
                this.showButton();
            }
        } else {
            this.showBanner();
        }
    }

    accept() {
        localStorage.setItem("cookie-consent", "accepted");
        this.consentGranted();
        this.hideBanner();
    }

    reject() {
        localStorage.setItem("cookie-consent", "rejected");
        gtag("consent", "update", {
            ad_user_data: 'denied',
            ad_personalization: 'denied',
            ad_storage: 'denied',
            analytics_storage: 'denied'
        });
        this.hideBanner();
        this.showButton();
        window.location.href = '/cookies';
    }

    consentGranted() {
        gtag("consent", "update", {
            ad_user_data: 'granted',
            ad_personalization: 'granted',
            ad_storage: 'granted',
            analytics_storage: 'granted'
        });
    }

    showBanner() {
        this.bannerTarget.classList.remove("hidden");
        this.hideButton();
    }

    hideBanner() {
        this.bannerTarget.classList.add("hidden");
    }


    showButton() {
        this.buttonTarget.classList.remove("hidden");
    }

    hideButton() {
        this.buttonTarget.classList.add("hidden");
    }
}
