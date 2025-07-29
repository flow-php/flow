import { Controller } from "@hotwired/stimulus";

/* stimulusFetch: 'lazy' */
export default class extends Controller {
    static targets = ["banner", "button", "status"];
    static values = {
        gaId: String
    }

    connect() {
        if (!this.isLocalStorageAvailable()) {
            return ;
        }

        const consentStatus = localStorage.getItem("cookie-consent");

        if (consentStatus) {
            this.bannerTarget.classList.add("hidden");
            // If accepted previously, inject GA
            if (consentStatus === "accepted") {
                this.consentGranted();
            }

            this.showButton();
        } else {
            this.showBanner();
        }
    }

    accept() {
        localStorage.setItem("cookie-consent", "accepted");
        this.consentGranted();
        this.hideBanner();
        this.showButton();
    }

    reject() {
        localStorage.setItem("cookie-consent", "rejected");
        gtag("consent", "update", {
            analytics_storage: 'denied'
        });
        this.hideBanner();
        this.showButton();
    }

    consentGranted() {
        if (document.querySelector('script[src="https://www.googletagmanager.com/gtag/js?id=' + this.gaIdValue + '"]')) {
            return;
        }

        const script = document.createElement('script');
        script.async = true;
        script.src = 'https://www.googletagmanager.com/gtag/js?id=' + this.gaIdValue;
        document.head.appendChild(script);

        gtag("consent", "update", {analytics_storage: 'granted'});
        gtag('js', new Date());
    }

    showBanner() {
        const status = localStorage.getItem("cookie-consent") === "accepted";

        if (status === true) {
            this.statusTarget.innerHTML = "Cookies Status: <strong class=\"text-green-600\">Accepted</strong>";
        }

        if (status === false) {
            this.statusTarget.innerHTML = "Cookies Status: <strong class=\"text-orange-600\">Rejected</strong>";
        }

        if (status === undefined) {
            this.statusTarget.innerHTML = "Cookies Status: <strong class=\"text-slate-400\">Undecided</strong>";
        }


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

    isLocalStorageAvailable() {
        try {
            localStorage.setItem('flow_test_local_storage', 'test');

            if (localStorage.getItem('flow_test_local_storage') !== 'test') {
                localStorage.removeItem('flow_test_local_storage');
                return false;
            }

            localStorage.removeItem('flow_test_local_storage');
            return true;
        } catch(e) {
            return false;
        }
    }
}
